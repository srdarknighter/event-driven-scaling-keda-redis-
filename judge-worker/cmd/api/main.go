package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"

	"judge-worker/internal/job"
	"judge-worker/internal/redisqueue"

	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr: getEnv("REDIS_ADDR", "redis:6379"),
	})
	defer rdb.Close()

	q := redisqueue.New(rdb, ctx)

	http.HandleFunc("/submit", func(w http.ResponseWriter, r *http.Request) {
		var j job.Job
		if err := json.NewDecoder(r.Body).Decode(&j); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("invalid request"))
			return
		}
		if err := q.Enqueue(j); err != nil {
			log.Println("enqueue error:", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)
	})

	log.Println("API running on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func getEnv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}
