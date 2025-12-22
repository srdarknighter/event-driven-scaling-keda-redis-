package job

import "time"

type ResultEvent struct {
	SubmissionID string    `db:"submission_id" json:"submission_id"`
	UserID       string    `db:"user_id" json:"user_id"`
	Tier         string    `db:"tier" json:"tier"`
	Language     string    `db:"language" json:"language"`
	ExecutionMs  int       `db:"execution_ms" json:"execution_ms"`
	Status       string    `db:"status" json:"status"`
	CompletedAt  time.Time `db:"completed_at" json:"completed_at"`
}
