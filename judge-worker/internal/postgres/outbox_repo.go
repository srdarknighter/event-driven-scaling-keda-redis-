package postgres

import (
	"judge-worker/internal/job"
	"time"

	"github.com/jmoiron/sqlx"
)

type OutboxEntry struct {
	ID           int64      `db:"id"`
	SubmissionID string     `db:"submission_id"`
	UserID       string     `db:"user_id"`
	Language     string     `db:"language"`
	Tier         string     `db:"tier"`
	Payload      []byte     `db:"payload"` // raw JSONB bytes
	Status       string     `db:"status"`
	CreatedAt    time.Time  `db:"created_at"`
	PublishedAt  *time.Time `db:"published_at"`
}

func InsertOutboxEntry(db *sqlx.DB, j job.Job, payload []byte) error {
	_, err := db.Exec(`
	INSERT INTO job_outbox(submission_id, user_id, language, tier, payload)
	VALUES ($1, $2, $3, $4, $5)
	ON CONFLICT (submission_id) DO NOTHING`,
		j.SubmissionID, j.UserID, j.Language, j.Tier, payload,
	)

	return err
}

func FetchAndLockPendingEntries(tx *sqlx.Tx, limit int) ([]OutboxEntry, error) {
	var entries []OutboxEntry
	err := tx.Select(&entries, `
		SELECT id, submission_id, user_id, language, tier, payload, status, created_at, published_at
		FROM job_outbox
		WHERE status = 'pending'
		ORDER BY id ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED`,
		limit,
	)

	return entries, err
}

func MarkOutboxEntryPublished(tx *sqlx.Tx, id int64) error {
	_, err := tx.Exec(`
	UPDATE job_outbox 
	SET status = 'published', 
	published_at = NOW() 
	WHERE id = $1`, id)
	return err
}
