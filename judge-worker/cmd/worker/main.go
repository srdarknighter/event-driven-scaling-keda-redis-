package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand/v2"
	"os"
	"time"

	"judge-worker/internal/job"
	"judge-worker/internal/stream"

	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr: getEnv("REDIS_ADDR", "redis:6379"),
	})
	defer rdb.Close()

	log.Println("Worker started")

	for {
		result, err := rdb.BRPop(ctx, 0, "free-submissions", "premium-submissions").Result()
		if err != nil {
			log.Println("Redis error:", err)
			continue
		}

		var j job.Job
		if err := json.Unmarshal([]byte(result[1]), &j); err != nil {
			log.Println("Unmarshal error:", err)
			continue
		}

		var res *job.ResultEvent
		if j.Tier == "premium" {
			res = processPremiumJob(j)
		} else {
			res = processFreeJob(j)
		}

		go func(re *job.ResultEvent) {
			if err := stream.PublishResult(ctx, rdb, re); err != nil {
				log.Println("publish failed:", err)
			}
		}(res)
	}
}

func processFreeJob(j job.Job) *job.ResultEvent {
	dur := time.Duration(5+rand.IntN(3)) * time.Second
	time.Sleep(dur)
	return createResultEvent(j, int(dur.Milliseconds()))
}

func processPremiumJob(j job.Job) *job.ResultEvent {
	dur := time.Duration(3+rand.IntN(2)) * time.Second
	time.Sleep(dur)
	return createResultEvent(j, int(dur.Milliseconds()))
}

func createResultEvent(j job.Job, durationMs int) *job.ResultEvent {
	return &job.ResultEvent{
		SubmissionID: j.SubmissionID,
		UserID:       j.UserID,
		Language:     j.Language,
		Tier:         j.Tier,
		ExecutionMs:  durationMs,
		Status:       "ACCEPTED",
		CompletedAt:  time.Now(),
	}
}

func getEnv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}
