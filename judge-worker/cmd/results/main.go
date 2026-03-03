package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"

	"judge-worker/internal/postgres"
	"judge-worker/internal/stream"
)

func main() {
	ctx := context.Background()

	db := postgres.New(getEnv("POSTGRES_DSN", "user=postgres password=postgres host=postgres dbname=leetcode sslmode=disable"))
	defer db.Close()
	postgres.Migrate(db)

	rdb := redis.NewClient(&redis.Options{
		Addr: getEnv("REDIS_ADDR", "redis:6379"),
	})
	defer rdb.Close()

	if err := stream.EnsureConsumerGroup(ctx, rdb, "submission", "results_group"); err != nil {
		log.Fatal(err)
	}

	consumerID := fmt.Sprintf("consumer-%s", uuid.NewString())
	log.Println("Results consumer started:", consumerID)

	drainPending(ctx, rdb, db, consumerID)

	for {
		streams, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    "results_group",
			Consumer: consumerID,
			Streams:  []string{"submission", ">"},
			Count:    10,
			Block:    5 * time.Second,
		}).Result()

		if err != nil {
			if err == redis.Nil {
				continue
			}
			log.Println("XREADGROUP error:", err)
			time.Sleep(time.Second)
			continue
		}

		for _, s := range streams {
			for _, msg := range s.Messages {
				processMessage(ctx, rdb, db, msg, consumerID)
			}
		}
	}
}

func processMessage(ctx context.Context, rdb *redis.Client, db *sqlx.DB, msg redis.XMessage, consumerID string) {
	ev, err := stream.MapToResultEvent(msg.Values)
	if err != nil {
		log.Printf("msg %s: map failed: %v — acking to discard malformed message", msg.ID, err)
		xack(ctx, rdb, msg.ID)
		return
	}

	if err := postgres.InsertResultEvent(db, ev); err != nil {
		log.Printf("msg %s: DB insert failed: %v — leaving in PEL", msg.ID, err)
		return
	}

	xack(ctx, rdb, msg.ID)
}

func drainPending(ctx context.Context, rdb *redis.Client, db *sqlx.DB, consumerID string) {
	log.Println("Draining pending results from previous session...")
	for {
		streams, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    "results_group",
			Consumer: consumerID,
			Streams:  []string{"submission", "0"},
			Count:    10,
			Block:    0,
		}).Result()

		if err != nil || len(streams) == 0 {
			break
		}

		msgs := streams[0].Messages
		if len(msgs) == 0 {
			break
		}
		for _, msg := range msgs {
			processMessage(ctx, rdb, db, msg, consumerID)
		}
	}
	log.Println("Pending drain complete")
}

func xack(ctx context.Context, rdb *redis.Client, id string) {
	if err := rdb.XAck(ctx, "submission", "results_group", id).Err(); err != nil {
		log.Printf("XAck failed for msg %s: %v", id, err)
	}
}

func getEnv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}
