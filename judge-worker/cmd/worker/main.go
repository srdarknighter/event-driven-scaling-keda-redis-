package main

import (
	"context"
	"encoding/json"
	"judge-worker/internal/job"
	"judge-worker/internal/postgres"
	"judge-worker/internal/stream"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
)

const (
	staleDuration = 90 * time.Second

	reaperDuration = 30 * time.Second
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		sig := <-sigCh
		log.Printf("Received signal %s, shutting down...\n", sig)
		cancel()
	}()

	streamName := getEnv("STREAM_NAME", "free-stream")
	groupName := getEnv("GROUP_NAME", "workers-free")
	workerTier := getEnv("WORKER_TIER", "free")

	consumerID, _ := os.Hostname()

	rdb := redis.NewClient(&redis.Options{
		Addr: getEnv("REDIS_ADDR", "redis:6379"),
	})
	defer rdb.Close()

	db := postgres.New(getEnv("POSTGRES_DSN", "user=postgres password=postgres host=postgres dbname=leetcode sslmode=disable"))
	defer db.Close()

	log.Printf("Worker started | tier=%s | stream=%s | consumer=%s\n", workerTier, streamName, groupName)

	drainPending(ctx, rdb, db, streamName, groupName, consumerID, workerTier)
	go reaper(ctx, rdb, db, streamName, groupName, consumerID, workerTier)

	for {
		select {
		case <-ctx.Done():
			log.Println("context cancellation received, exiting worker gracefully")
			return
		default:
		}

		streams, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerID,
			Streams:  []string{streamName, ">"},
			Count:    1,
			Block:    5 * time.Second,
		}).Result()

		if err != nil {
			if err == context.Canceled {
				log.Println("XREADGROUP unblocked by context cancel - shutting down")
				return
			}
			if err == redis.Nil {
				continue
			}
			log.Println("XREADGROUP error:", err)
			time.Sleep(time.Second)
			continue
		}

		for _, s := range streams {
			for _, msgs := range s.Messages {
				processMessage(ctx, rdb, db, msgs, streamName, groupName, consumerID, workerTier)
			}
		}
	}
}

func drainPending(ctx context.Context, rdb *redis.Client, db *sqlx.DB, streamName, groupName, consumerId, workerTier string) {
	log.Println("Draining pending messages from previous session...")

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		streams, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerId,
			Streams:  []string{streamName, "0"},
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
			processMessage(ctx, rdb, db, msg, streamName, groupName, consumerId, workerTier)
		}
	}
	log.Println("Pending Drain Complete")
}

func processMessage(ctx context.Context, rdb *redis.Client, db *sqlx.DB, msg redis.XMessage, streamName, groupName, consumerId, workerTier string) {
	payloadStr, ok := msg.Values["payload"].(string)
	if !ok {
		log.Printf("msg %v: missing payload field, acking to discard", msg.ID)
		xack(ctx, rdb, streamName, groupName, msg.ID)
		return
	}

	var j job.Job

	if err := json.Unmarshal([]byte(payloadStr), &j); err != nil {
		log.Printf("msg %v: unmarshal failed: %v, acking to discard", msg.ID, err)
		xack(ctx, rdb, streamName, groupName, msg.ID)
		return
	}

	err := postgres.ClaimJob(db, j.SubmissionID, consumerId)

	if err != nil {
		if err == postgres.ErrAlreadyClaimed {
			rescueResult(ctx, rdb, db, j.SubmissionID, msg, streamName, groupName)
			return
		}
		log.Printf("msg %v: claim error: %s leaving in PEL", msg.ID, err)
		return
	}

	var res *job.ResultEvent
	switch workerTier {
	case "free":
		res = processFreeJob(j)
	case "premium":
		res = processPremiumJob(j)
	default:
		log.Printf("msg %v: unknown worker tier %s, acking to discard", msg.ID, workerTier)
		xack(ctx, rdb, streamName, groupName, msg.ID)
		return
	}

	if err := postgres.SaveJobResult(db, j.SubmissionID, res); err != nil {
		log.Printf("msg %s: SaveJobResult failed: %v — leaving in PEL", msg.ID, err)
		return
	}

	if err := stream.PublishResult(ctx, rdb, res); err != nil {
		log.Printf("PublishResult failed for msg %s: %v", msg.ID, err)
	}

	xack(ctx, rdb, streamName, groupName, msg.ID)
	log.Printf("msg %v: acked, submission %s done", msg.ID, j.SubmissionID)
}

func rescueResult(ctx context.Context, rdb *redis.Client, db *sqlx.DB, submissionID string, msg redis.XMessage, streamName, groupName string) {
	res, err := postgres.GetJobResult(db, submissionID)
	if err != nil {
		log.Printf("msg %s: GetJobResult failed: %v — leaving in PEL", msg.ID, err)
		return
	}

	if res == nil {
		log.Printf("msg %s: submission %s claimed but result not yet stored — discarding", msg.ID, submissionID)
		xack(ctx, rdb, streamName, groupName, msg.ID)
		return
	}

	if err := postgres.InsertResultEvent(db, res); err != nil {
		log.Printf("msg %s: rescue InsertResultEvent failed: %v — leaving in PEL", msg.ID, err)
		return
	}

	xack(ctx, rdb, streamName, groupName, msg.ID)
	log.Printf("msg %s: rescued, submission %s done", msg.ID, submissionID)
}

func reaper(ctx context.Context, rdb *redis.Client, db *sqlx.DB, streamName, groupName, consumerID, workerTier string) {
	ticker := time.NewTicker(reaperDuration)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("context cancellation received, exiting reaper gracefully")
			return
		case <-ticker.C:
			claimed, _, err := rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
				Stream:   streamName,
				Group:    groupName,
				Consumer: consumerID,
				MinIdle:  staleDuration,
				Start:    "0-0",
				Count:    10,
			}).Result()

			if err != nil {
				if err == context.Canceled {
					return
				}
				log.Println("XAUTOCLAIM error: ", err)
				continue
			}

			for _, msg := range claimed {
				processMessage(ctx, rdb, db, msg, streamName, groupName, consumerID, workerTier)
			}
		}
	}
}

func xack(ctx context.Context, rdb *redis.Client, streamName, groupName, id string) {
	if err := rdb.XAck(ctx, streamName, groupName, id).Err(); err != nil {
		log.Printf("XAck failed for msg %s: %v", id, err)
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
