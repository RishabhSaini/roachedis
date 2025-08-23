package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	_ "github.com/lib/pq"
)

// --- Data Structures ---
type LogEntry struct {
	Key       string    `json:"key"`
	Value     string    `json:"value"`
	Timestamp time.Time `json:"timestamp"`
	Deleted   bool      `json:"deleted"`
}

// --- Global Components ---
var (
	db          *sql.DB
	redisClient *redis.Client
	ctx         = context.Background()
	keyLocks    sync.Map
)

// --- Database Interaction (CockroachDB) ---
func initDB(dbConnectionString string) {
	var err error
	db, err = sql.Open("postgres", dbConnectionString)
	if err != nil {
		log.Fatalf("Failed to connect to CockroachDB: %v", err)
	}
	// Enable CHANGEFEED on the table
	createTableSQL := `
    CREATE TABLE IF NOT EXISTS kv_log (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        key STRING NOT NULL,
        value STRING,
        timestamp TIMESTAMPTZ NOT NULL,
        deleted BOOL DEFAULT FALSE,
		FAMILY "primary" (id, key, value, timestamp, deleted)
    );
	ALTER TABLE kv_log CONFIGURE ZONE USING gc.ttlseconds = 3600; -- Optional: Clean up old log entries
    CREATE INDEX IF NOT EXISTS idx_key_timestamp ON kv_log (key, timestamp DESC);
    `
	if _, err := db.Exec(createTableSQL); err != nil {
		log.Fatalf("Failed to create kv_log table in CockroachDB: %v", err)
	}
	log.Println("CockroachDB connection successful and table initialized.")
}

func appendToLog(entry LogEntry) error {
	sqlStatement := `INSERT INTO kv_log (key, value, timestamp, deleted) VALUES ($1, $2, $3, $4)`
	_, err := db.Exec(sqlStatement, entry.Key, entry.Value, entry.Timestamp, entry.Deleted)
	return err
}

func getLatestValueFromLog(key string) (string, bool, error) {
	var value string
	var deleted bool
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
			return "", false, nil
		}
		return "", false, err
	}
	if deleted {
		return "", false, nil
	}
	return value, true, nil
}

// --- Cache Interaction ---
func initRedis(redisAddress string) {
	redisClient = redis.NewClient(&redis.Options{
		Addr: redisAddress,
	})
	if _, err := redisClient.Ping(ctx).Result(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	log.Println("Redis connection successful.")
}

// --- API Handlers ---
func handlePut(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	var payload struct {
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	entry := LogEntry{
		Key:       key,
		Value:     payload.Value,
		Timestamp: time.Now().UTC(),
		Deleted:   false,
	}
	// The server's ONLY job on a write is to append to the log.
	// The CDC service will handle updating the cache.
	if err := appendToLog(entry); err != nil {
		log.Printf("ERROR: Failed to write to CockroachDB for key '%s': %v", key, err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	log.Printf("PUT successful for key: %s (persisted to log)", key)
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(entry)
}

func handleGet(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	val, err := redisClient.Get(ctx, key).Result()
	if err == nil {
		log.Printf("GET cache hit for key: %s", key)
		json.NewEncoder(w).Encode(map[string]string{"key": key, "value": val})
		return
	}
	log.Printf("GET cache miss for key: %s. Querying CockroachDB.", key)
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
	// We still populate the cache on a miss for subsequent reads.
	if err := redisClient.Set(ctx, key, dbValue, 0).Err(); err != nil {
		log.Printf("ERROR: Failed to populate cache for key '%s': %v", key, err)
	}
	log.Printf("GET successful from CockroachDB for key: %s", key)
	json.NewEncoder(w).Encode(map[string]string{"key": key, "value": dbValue})
}

func handleDelete(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	entry := LogEntry{
		Key:       key,
		Value:     "",
		Timestamp: time.Now().UTC(),
		Deleted:   true,
	}
	// The server's ONLY job on a delete is to write a tombstone to the log.
	if err := appendToLog(entry); err != nil {
		log.Printf("ERROR: Failed to write delete log to CockroachDB for key '%s': %v", key, err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	log.Printf("DELETE successful for key: %s (tombstone persisted to log)", key)
	w.WriteHeader(http.StatusOK)
}

func main() {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgresql://root@localhost:26257/defaultdb?sslmode=disable"
	}
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "localhost:6379"
	}
	serverPort := os.Getenv("PORT")
	if serverPort == "" {
		serverPort = "8080"
	}
	log.Printf("Connecting to Database at: %s", dbURL)
	log.Printf("Connecting to Redis at: %s", redisURL)
	initDB(dbURL)
	initRedis(redisURL)
	defer db.Close()
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
	log.Printf("Starting server on port :%s", serverPort)
	if err := http.ListenAndServe(":"+serverPort, nil); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
