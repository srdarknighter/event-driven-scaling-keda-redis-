package main

import (
	"log"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type ResultEvent struct {
	SubmissionID string    `db:"submission_id"`
	UserID       string    `db:"user_id"`
	Tier         string    `db:"tier"`
	Language     string    `db:"language"`
	ExecutionMs  int       `db:"execution_ms"`
	Status       string    `db:"status"`
	CompletedAt  time.Time `db:"completed_at"`
}

var schema = `
CREATE TABLE IF NOT EXISTS submissions (
    submission_id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255),
    tier VARCHAR(255),
    language VARCHAR(255),
    execution_ms INT,
    status VARCHAR(255),
    completed_at TIMESTAMP
);
`

func initDB() *sqlx.DB {
	db, err := sqlx.Connect("postgres", "user=postgres password=postgres host=localhost dbname=leetcode sslmode=disable")
	if err != nil {
		log.Fatalln(err)
	}
	return db
}

func createSchema(db *sqlx.DB) {
	db.MustExec(schema)
}

func insertResultEvent(db *sqlx.DB, r ResultEvent) error {
	query := `INSERT INTO submissions
        (submission_id, user_id, tier, language, execution_ms, status, completed_at)
        VALUES
        (:submission_id, :user_id, :tier, :language, :execution_ms, :status, :completed_at)
        ON CONFLICT (submission_id) DO NOTHING
		`
	_, err := db.NamedExec(query, r)
	return err
}
