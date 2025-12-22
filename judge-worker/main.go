package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
)

type Job struct {
	SubmissionID string `json:"submission_id"`
	UserId       string `json:"user_id"`
	Language     string `json:"language"`
	Tier         string `json:"tier"`
}

var ctx = context.Background()

func main() {
	db := initDB()
	defer db.Close()
	createSchema(db)

	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	queueName := getEnv("QUEUE_NAME", "validation")

	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	err := ensureConsumerGroup(rdb, "submission", "results_group")
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		consumeRedisStream(rdb, db, "submission", "results_group")
	}()

	defer rdb.Close()
	startHTTPProducer(rdb)

	log.Println("Judge worker Started")
	log.Println("Waiting for jobs on queue", queueName)

	for {
		result, err := rdb.BRPop(ctx, 0*time.Second, "free-submissions", "premium-submissions").Result()
		if err != nil {
			log.Println("Redis Error", err)
			continue
		}

		var job Job
		jobJson := result[1]
		if err := json.Unmarshal([]byte(jobJson), &job); err != nil {
			log.Println("Error unmarshalling job", err)
			continue
		}
		var job_result *ResultEvent
		if job.Tier == "premium" {
			job_result = processPremiumJob(job)
		} else {
			job_result = processFreeJob(job)
		}

		go func(res *ResultEvent) {
			if err := publishToRedisStream(rdb, res); err != nil {
				log.Println("publish failed", err)
			}
		}(job_result)
	}
}

func consumeRedisStream(rdb *redis.Client, db *sqlx.DB, stream string, group string) {
	consumer := fmt.Sprintf("consumer-%s", uuid.New().String())
	log.Println("Starting consumer:", consumer)

	for {
		streams, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams:  []string{stream, ">"},
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
				processAndAck(rdb, db, stream, group, msg)
			}
		}
	}
}

func processAndAck(rdb *redis.Client, db *sqlx.DB, stream string, group string, msg redis.XMessage) {
	event, err := mapToResultEvent(msg.Values)
	if err != nil {
		log.Println("Mapping failed:", err)
		return
	}

	if err := insertResultEvent(db, &event); err != nil {
		log.Println("DB insert failed:", err)
		return
	}

	if err := rdb.XAck(ctx, stream, group, msg.ID).Err(); err != nil {
		log.Println("ACK failed:", err)
	}
}

func ensureConsumerGroup(rdb *redis.Client, stream string, group string) error {
	err := rdb.XGroupCreateMkStream(ctx, stream, group, "0").Err()

	if err != nil {
		if strings.Contains(err.Error(), "BUSYGROUP") {
			log.Println("Consumer group already exists")
			return nil
		}
		return err
	}

	log.Println("Consumer group created:", group)
	return nil
}

func mapToResultEvent(values map[string]interface{}) (ResultEvent, error) {
	getStr := func(k string) string {
		if v, ok := values[k].(string); ok {
			return v
		}
		return ""
	}

	execMs, _ := strconv.Atoi(getStr("execution_ms"))
	completedAt, err := time.Parse(time.RFC3339, getStr("completed_at"))
	if err != nil {
		completedAt = time.Now()
	}

	return ResultEvent{
		SubmissionID: getStr("submission_id"),
		UserID:       getStr("user_id"),
		Tier:         getStr("tier"),
		Language:     getStr("language"),
		ExecutionMs:  execMs,
		Status:       getStr("status"),
		CompletedAt:  completedAt,
	}, nil
}

func processFreeJob(job Job) *ResultEvent {
	var duration time.Duration
	log.Println("Processing job: ", job)
	duration = time.Duration(5+rand.IntN(3)) * time.Second
	time.Sleep(duration)

	result := createResultEvent(job, int(duration))

	log.Println("Completed job")
	return result
}

func processPremiumJob(job Job) *ResultEvent {
	var duration time.Duration
	log.Println("Processing job: ", job)
	duration = time.Duration(3+rand.IntN(2)) * time.Second
	time.Sleep(duration)

	result := createResultEvent(job, int(duration))

	log.Println("Completed job")
	return result
}

func createResultEvent(job Job, duration int) *ResultEvent {
	var resultEvent ResultEvent
	resultEvent.SubmissionID = job.SubmissionID
	resultEvent.UserID = job.UserId
	resultEvent.Language = job.Language
	resultEvent.Tier = job.Tier
	resultEvent.ExecutionMs = duration
	resultEvent.Status = "ACCEPTED"
	resultEvent.CompletedAt = time.Now()
	return &resultEvent
}

func publishToRedisStream(rdb *redis.Client, resultEvent *ResultEvent) error {
	_, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "submission",
		Values: map[string]interface{}{
			"submission_id": resultEvent.SubmissionID,
			"user_id":       resultEvent.UserID,
			"tier":          resultEvent.Tier,
			"language":      resultEvent.Language,
			"execution_ms":  resultEvent.ExecutionMs,
			"status":        resultEvent.Status,
			"completed_at":  resultEvent.CompletedAt.Unix(),
		},
		ID: "*",
	}).Result()

	return err
}

func getEnv(key string, defVal string) string {
	val, ok := os.LookupEnv(key)
	if ok {
		return val
	}
	return defVal
}

func startHTTPProducer(rdb *redis.Client) {
	http.HandleFunc("/submit", func(w http.ResponseWriter, r *http.Request) {
		var job Job
		err := json.NewDecoder(r.Body).Decode(&job)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("invalid request"))
			return
		}

		queueName := "free-submissions"
		if job.Tier == "premium" {
			queueName = "premium-submissions"
		}

		data, _ := json.Marshal(job)
		rdb.LPush(ctx, queueName, data)
		w.WriteHeader(http.StatusCreated)
	})

	log.Println("HTTP Producer running on :8080")
	go http.ListenAndServe(":8080", nil)
}
