package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"os"

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
	// Get connection details from environment variables
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL environment variable is not set")
	}
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		log.Fatal("REDIS_URL environment variable is not set")
	}

	// Connect to Redis
	redisClient = redis.NewClient(&redis.Options{Addr: redisURL})
	if _, err := redisClient.Ping(ctx).Result(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	log.Println("Cache Hydrator connected to Redis.")

	// Connect to CockroachDB
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatalf("Failed to connect to CockroachDB: %v", err)
	}
	defer db.Close()
	log.Println("Cache Hydrator connected to CockroachDB.")

	// The CREATE CHANGEFEED statement
	// We select only the columns we need to minimize data transfer
	changefeedQuery := `CREATE CHANGEFEED FOR TABLE kv_log WITH updated, resolved, format = json, envelope = wrapped`

	log.Println("Starting CockroachDB changefeed...")
	rows, err := db.Query(changefeedQuery)
	if err != nil {
		log.Fatalf("Failed to create changefeed: %v", err)
	}
	defer rows.Close()

	// Loop forever, processing messages from the changefeed
	for rows.Next() {
		var topic string
		var key []byte // The primary key of the row, which we don't need
		var value []byte // The JSON payload of the row change

		if err := rows.Scan(&topic, &key, &value); err != nil {
			log.Printf("Error scanning changefeed row: %v", err)
			continue
		}

		// We only care about the row data, not other changefeed messages
		if value == nil {
			continue
		}

		// Unmarshal the JSON payload from the changefeed
		var msg ChangefeedMessage
		if err := json.Unmarshal(value, &msg); err != nil {
			log.Printf("Error unmarshaling changefeed message: %v", err)
			continue
		}

		// Update Redis based on the message
		if msg.Deleted {
			log.Printf("CDC Event: Deleting key '%s' from Redis.", msg.Key)
			redisClient.Del(ctx, msg.Key)
		} else {
			log.Printf("CDC Event: Setting key '%s' in Redis.", msg.Key)
			redisClient.Set(ctx, msg.Key, msg.Value, 0)
		}
	}
}
