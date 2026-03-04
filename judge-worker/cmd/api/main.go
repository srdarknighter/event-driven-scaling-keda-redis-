package main

import (
	"encoding/json"
	"judge-worker/internal/job"
	"judge-worker/internal/postgres"
	"log"
	"net/http"
	"os"
)

func main() {
	db := postgres.New(getEnv("POSTGRES_DSN", "user=postgres password=postgres host=postgres dbname=leetcode sslmode=disable"))
	defer db.Close()
	postgres.Migrate(db)

	http.HandleFunc("/submit", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var j job.Job
		if err := json.NewDecoder(r.Body).Decode(&j); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("Invalid request body"))
			return
		}

		if j.SubmissionID == "" || j.UserID == "" || j.Language == "" || j.Tier == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("missing required fields"))
			return
		}

		payload, err := json.Marshal(j)
		if err != nil {
			log.Println("marshal error:", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		if err := postgres.InsertOutboxEntry(db, j, payload); err != nil {
			log.Println("outboxinsert error:", err)
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
