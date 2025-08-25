package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/go-redis/redis/v8"
	_ "github.com/lib/pq"
)

var (
	redisClient *redis.Client
	ctx         = context.Background()
)

// Represents the structure of the JSON message from the changefeed
type ChangefeedMessage struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	Deleted bool   `json:"deleted"`
}

func main() {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL environment variable is not set")
	}
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		log.Fatal("REDIS_URL environment variable is not set")
	}

	redisClient = redis.NewClient(&redis.Options{Addr: redisURL})
	if _, err := redisClient.Ping(ctx).Result(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	log.Println("Cache Hydrator connected to Redis.")

	var db *sql.DB
	var err error
	maxRetries := 10
	retryDelay := 2 * time.Second

	for i := 0; i < maxRetries; i++ {
		db, err = sql.Open("postgres", dbURL)
		if err == nil {
			err = db.Ping()
			if err == nil {
				log.Println("Cache Hydrator connected to CockroachDB.")
				break
			}
		}
		log.Printf("Could not connect to CockroachDB, retrying in %v... (%d/%d)", retryDelay, i+1, maxRetries)
		time.Sleep(retryDelay)
	}

	if err != nil {
		log.Fatalf("Failed to connect to CockroachDB after %d retries: %v", maxRetries, err)
	}
	defer db.Close()

	// Hydrator is now responsible for creating the table
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
	log.Println("Table 'kv_log' ensured to exist.")

	log.Println("Ensuring kv.rangefeed.enabled is set to true...")
	_, err = db.Exec("SET CLUSTER SETTING kv.rangefeed.enabled = true;")
	if err != nil {
		log.Printf("Could not enable kv.rangefeed.enabled (might already be set): %v", err)
	}

	changefeedQuery := `CREATE CHANGEFEED FOR TABLE kv_log WITH updated, resolved, format = json, envelope = wrapped`

	log.Println("Starting CockroachDB changefeed...")
	rows, err := db.Query(changefeedQuery)
	if err != nil {
		log.Fatalf("Failed to create changefeed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		// Use sql.NullString to handle checkpoint messages where value is NULL
		var topic sql.NullString
		var key sql.NullString // The primary key (UUID) as a string
		var value sql.NullString // The JSON payload as a string

		if err := rows.Scan(&topic, &key, &value); err != nil {
			log.Printf("Error scanning changefeed row: %v", err)
			continue
		}

		// We only care about data rows, which have a valid 'value' payload.
		// This safely ignores the checkpoint messages where 'value' is NULL.
		if !value.Valid {
			continue
		}

		var msg ChangefeedMessage
		// Unmarshal the valid JSON string from the changefeed
		if err := json.Unmarshal([]byte(value.String), &msg); err != nil {
			log.Printf("Error unmarshaling changefeed message: %v", err)
			continue
		}

		if msg.Deleted {
			log.Printf("CDC Event: Deleting key '%s' from Redis.", msg.Key)
			redisClient.Del(ctx, msg.Key)
		} else {
			log.Printf("CDC Event: Setting key '%s' in Redis.", msg.Key)
			redisClient.Set(ctx, msg.Key, msg.Value, 0)
		}
	}
}
