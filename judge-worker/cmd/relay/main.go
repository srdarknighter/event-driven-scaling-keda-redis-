package main

import (
	"context"
	"encoding/json"
	"judge-worker/internal/job"
	"judge-worker/internal/postgres"
	"log"
	"os"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
)

const (
	pollInterval = 2 * time.Second
	batchSize    = 50
)

func main() {
	ctx := context.Background()

	db := postgres.New(getEnv("POSTGRES_DSN", "user=postgres password=postgres host=postgres dbname=leetcode sslmode=disable"))
	defer db.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr: getEnv("REDIS_ADDR", "redis:6379"),
	})
	defer rdb.Close()

	for _, s := range []struct{ stream, group string }{
		{"free-stream", "workers-free"},
		{"premium-stream", "workers-premium"},
	} {
		err := rdb.XGroupCreateMkStream(ctx, s.stream, s.group, "0").Err()
		if err != nil && !isAlreadyExists(err) {
			log.Fatalf("create group %s: %v", s.group, err)
		}
	}

	log.Println("Relay started, polling outbox every", pollInterval)

	for {
		if err := poll(ctx, db, rdb); err != nil {
			log.Println("poll error:", err)
		}
		time.Sleep(pollInterval)
	}
}

func poll(ctx context.Context, db *sqlx.DB, rdb *redis.Client) error {
	tx, err := db.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	entries, err := postgres.FetchAndLockPendingEntries(tx, batchSize)
	if err != nil {
		return err
	}

	if len(entries) == 0 {
		return nil
	}

	for _, e := range entries {
		var j job.Job
		if err := json.Unmarshal(e.Payload, &j); err != nil {
			log.Printf("unmarshal outbox entry %d: %v - skipping", e.ID, err)
			continue
		}

		stream := "free-stream"
		if j.Tier == "premium" {
			stream = "premium-stream"
		}

		_, err := rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: stream,
			ID:     "*",
			Values: map[string]interface{}{
				"submission_id": j.SubmissionID,
				"payload":       string(e.Payload),
			},
		}).Result()

		if err != nil {
			log.Printf("XADD failed for submission %s: %v", e.SubmissionID, err)
			return err
		}

		if err := postgres.MarkOutboxEntryPublished(tx, e.ID); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func isAlreadyExists(err error) bool {
	return err != nil && err.Error() == "BUSYGROUP Consumer Group name already exists"
}

func getEnv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}
