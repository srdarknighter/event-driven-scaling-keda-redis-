package postgres

import (
	"database/sql"
	"encoding/json"
	"errors"
	"judge-worker/internal/job"

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

func SaveJobResult(db *sqlx.DB, submissionID string, r *job.ResultEvent) error {
	payload, err := json.Marshal(r)
	if err != nil {
		return err
	}

	_, err = db.Exec(`
	UPDATE processed_jobs
	SET result_payload = $1, result_saved_at = NOW()
	WHERE submission_id = $2`,
		payload, submissionID)

	return err
}

func GetJobResult(db *sqlx.DB, submissionID string) (*job.ResultEvent, error) {
	var payload []byte

	err := db.QueryRow(`
	SELECT result_payload
	FROM processed_jobs
	WHERE submission_id = $1`,
		submissionID).Scan(&payload)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	if payload == nil {
		return nil, nil
	}

	var r job.ResultEvent
	if err := json.Unmarshal(payload, &r); err != nil {
		return nil, err
	}

	return &r, err
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
