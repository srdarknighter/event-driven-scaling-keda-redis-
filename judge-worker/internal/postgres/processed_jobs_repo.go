package postgres

import (
	"database/sql"
	"errors"

	"github.com/jmoiron/sqlx"
)

var ErrAlreadyClaimed = errors.New("job already claimed")

func ClaimJob(db *sqlx.DB, submission_id, workerID string) error {
	result, err := db.Exec(`
	INSERT INTO processed_jobs (submission_id, worker_id)
	VALUES ($1, $2)
	ON CONFLICT (submission_id) DO NOTHING`,
		submission_id, workerID)

	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()

	if rows == 0 {
		return ErrAlreadyClaimed
	}
	return nil
}

func IsJobProcessed(db *sqlx.DB, submission_id string) (bool, error) {
	var id string
	err := db.Get(&id, `
		SELECT submission_id
		FROM processed_jobs
		WHERE submission_id = $1`,
		submission_id)

	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}

	return err == nil, err
}
