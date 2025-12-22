package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/google/uuid"
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

	consumer := fmt.Sprintf("consumer-%s", uuid.NewString())
	log.Println("Results consumer started:", consumer)

	for {
		streams, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    "results_group",
			Consumer: consumer,
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
				ev, err := stream.MapToResultEvent(msg.Values)
				if err != nil {
					log.Println("map failed:", err)
					continue
				}
				if err := postgres.InsertResultEvent(db, ev); err != nil {
					log.Println("DB insert failed:", err)
					continue
				}
				if err := rdb.XAck(ctx, "submission", "results_group", msg.ID).Err(); err != nil {
					log.Println("ACK failed:", err)
				}
			}
		}
	}
}

func getEnv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}
