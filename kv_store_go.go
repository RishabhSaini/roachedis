package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	_ "github.com/lib/pq" // This is the standard PostgreSQL driver, which works perfectly with CockroachDB.
)

// --- Configuration ---

// This connection string is formatted for a CockroachDB cluster.
// You might be connecting to a single node for local development or a load balancer for a multi-node cluster.
// Example for a secure CockroachDB cluster:
// "postgresql://user:password@host:26257/kvstore?sslmode=verify-full&sslrootcert=certs/ca.crt"
// Example for an insecure local CockroachDB node:
const (
	dbConnectionString = "postgresql://root@localhost:26257/defaultdb?sslmode=disable"
	redisAddress       = "localhost:6379"
	serverPort         = ":8080"
)

// --- Data Structures ---

// LogEntry represents a single change in our key-value store.
// It's the structure we'll store in our persistent log in CockroachDB.
type LogEntry struct {
	Key       string    `json:"key"`
	Value     string    `json:"value"`
	Timestamp time.Time `json:"timestamp"`
	Deleted   bool      `json:"deleted"` // To handle deletes as a log entry
}

// --- Global Components ---
var (
	db          *sql.DB
	redisClient *redis.Client
	ctx         = context.Background()
	// A mutex to prevent race conditions during cache misses,
	// where multiple goroutines might try to query the DB and write to the cache simultaneously.
	keyLocks sync.Map
)

// --- Database Interaction (CockroachDB) ---

// initDB initializes the connection to the CockroachDB cluster
// and creates the necessary table if it doesn't exist.
func initDB() {
	var err error
	// The "postgres" driver name is correct here due to CockroachDB's wire compatibility.
	db, err = sql.Open("postgres", dbConnectionString)
	if err != nil {
		log.Fatalf("Failed to connect to CockroachDB: %v", err)
	}

	// CockroachDB automatically handles replication and consensus for this table.
	createTableSQL := `
    CREATE TABLE IF NOT EXISTS kv_log (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        key STRING NOT NULL,
        value STRING,
        timestamp TIMESTAMPTZ NOT NULL,
        deleted BOOL DEFAULT FALSE
    );
    CREATE INDEX IF NOT EXISTS idx_key_timestamp ON kv_log (key, timestamp DESC);
    `
	if _, err := db.Exec(createTableSQL); err != nil {
		log.Fatalf("Failed to create kv_log table in CockroachDB: %v", err)
	}

	log.Println("CockroachDB connection successful and table initialized.")
}

// appendToLog writes a new entry to our persistent, append-only log in CockroachDB.
// CockroachDB's transactional guarantees ensure this is an atomic operation.
func appendToLog(entry LogEntry) error {
	sqlStatement := `INSERT INTO kv_log (key, value, timestamp, deleted) VALUES ($1, $2, $3, $4)`
	_, err := db.Exec(sqlStatement, entry.Key, entry.Value, entry.Timestamp, entry.Deleted)
	return err
}

// getLatestValueFromLog retrieves the most recent value for a key from CockroachDB.
// This is our fallback when the cache misses.
func getLatestValueFromLog(key string) (string, bool, error) {
	var value string
	var deleted bool

	// Query for the most recent non-deleted entry for the given key.
	sqlStatement := `
    SELECT value, deleted FROM kv_log
    WHERE key = $1
    ORDER BY timestamp DESC
    LIMIT 1;
    `
	row := db.QueryRow(sqlStatement, key)
	err := row.Scan(&value, &deleted)

	if err != nil {
		if err == sql.ErrNoRows {
			return "", false, nil // Key not found
		}
		return "", false, err // Other database error
	}

	if deleted {
		return "", false, nil // Key was explicitly deleted
	}

	return value, true, nil
}

// --- Cache Interaction ---

// initRedis initializes the connection to the Redis cache.
func initRedis() {
	redisClient = redis.NewClient(&redis.Options{
		Addr: redisAddress,
	})

	// Ping the server to check the connection.
	if _, err := redisClient.Ping(ctx).Result(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	log.Println("Redis connection successful.")
}

// --- API Handlers ---

// handlePut handles writing a key-value pair.
func handlePut(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		http.Error(w, "Key is required", http.StatusBadRequest)
		return
	}

	var payload struct {
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	if payload.Value == "" {
		http.Error(w, "Value is required", http.StatusBadRequest)
		return
	}

	// 1. Create the log entry.
	entry := LogEntry{
		Key:       key,
		Value:     payload.Value,
		Timestamp: time.Now().UTC(),
		Deleted:   false,
	}

	// 2. Append to the persistent log (CockroachDB).
	if err := appendToLog(entry); err != nil {
		log.Printf("ERROR: Failed to write to CockroachDB for key '%s': %v", key, err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// 3. Update the Redis cache.
	if err := redisClient.Set(ctx, key, payload.Value, 0).Err(); err != nil {
		log.Printf("ERROR: Failed to update cache for key '%s': %v", key, err)
	}

	log.Printf("PUT successful for key: %s", key)
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(entry)
}

// handleGet handles reading a key-value pair.
func handleGet(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		http.Error(w, "Key is required", http.StatusBadRequest)
		return
	}

	// 1. Check Redis cache first.
	val, err := redisClient.Get(ctx, key).Result()
	if err == nil {
		log.Printf("GET cache hit for key: %s", key)
		json.NewEncoder(w).Encode(map[string]string{"key": key, "value": val})
		return
	}

	if err != redis.Nil {
		log.Printf("ERROR: Redis error for key '%s': %v", key, err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// --- Cache Miss ---
	mu, _ := keyLocks.LoadOrStore(key, &sync.Mutex{})
	mu.(*sync.Mutex).Lock()
	defer mu.(*sync.Mutex).Unlock()
	defer keyLocks.Delete(key)

	val, err = redisClient.Get(ctx, key).Result()
	if err == nil {
		log.Printf("GET cache hit (after lock) for key: %s", key)
		json.NewEncoder(w).Encode(map[string]string{"key": key, "value": val})
		return
	}

	log.Printf("GET cache miss for key: %s. Querying CockroachDB.", key)

	// 2. Fallback to the persistent log (CockroachDB).
	dbValue, found, err := getLatestValueFromLog(key)
	if err != nil {
		log.Printf("ERROR: CockroachDB query failed for key '%s': %v", key, err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	if !found {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	// 3. Populate the cache.
	if err := redisClient.Set(ctx, key, dbValue, 0).Err(); err != nil {
		log.Printf("ERROR: Failed to populate cache for key '%s': %v", key, err)
	}

	log.Printf("GET successful from CockroachDB for key: %s", key)
	json.NewEncoder(w).Encode(map[string]string{"key": key, "value": dbValue})
}

// handleDelete handles deleting a key.
func handleDelete(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		http.Error(w, "Key is required", http.StatusBadRequest)
		return
	}

	// 1. Create a "tombstone" entry in the log.
	entry := LogEntry{
		Key:       key,
		Value:     "",
		Timestamp: time.Now().UTC(),
		Deleted:   true,
	}

	if err := appendToLog(entry); err != nil {
		log.Printf("ERROR: Failed to write delete log to CockroachDB for key '%s': %v", key, err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// 2. Invalidate the cache.
	if err := redisClient.Del(ctx, key).Err(); err != nil {
		log.Printf("ERROR: Failed to invalidate cache for key '%s': %v", key, err)
	}

	log.Printf("DELETE successful for key: %s", key)
	w.WriteHeader(http.StatusOK)
}

func main() {
	// Initialize database (CockroachDB) and Redis connections
	initDB()
	initRedis()
	defer db.Close()

	// Register handlers
	http.HandleFunc("/kv/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			handleGet(w, r)
		case http.MethodPut:
			handlePut(w, r)
		case http.MethodDelete:
			handleDelete(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	// Start the server
	log.Printf("Starting server on port %s", serverPort)
	if err := http.ListenAndServe(serverPort, nil); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
