package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"time"

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
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	queueName := getEnv("QUEUE_NAME", "submissions")

	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	startHTTPProducer(rdb)

	log.Println("Judge worker Started")
	log.Println("Waiting for jobs on queue", queueName)

	for {
		result, err := rdb.BRPop(ctx, 0*time.Second, queueName).Result()
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

		if job.Tier == "premium" {
			processPremiumJob(job)
		} else {
			processFreeJob(job)
		}
	}
}

func processFreeJob(job Job) {
	var duration time.Duration
	log.Println("Processing job: ", job)
	duration = time.Duration(5+rand.IntN(3)) * time.Second
	time.Sleep(duration)
	log.Println("Completed job")
}

func processPremiumJob(job Job) {
	var duration time.Duration
	log.Println("Processing job: ", job)
	duration = time.Duration(3+rand.IntN(2)) * time.Second
	time.Sleep(duration)
	log.Println("Completed job")
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
			w.Write([]byte("inavlid request"))
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
