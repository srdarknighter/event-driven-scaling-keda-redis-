package postgres

import (
	"judge-worker/internal/job"

	"github.com/jmoiron/sqlx"
)

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

func Migrate(db *sqlx.DB) {
	db.MustExec(schema)
}

func InsertResultEvent(db *sqlx.DB, r *job.ResultEvent) error {
	query := `INSERT INTO submissions
        (submission_id, user_id, tier, language, execution_ms, status, completed_at)
        VALUES
        (:submission_id, :user_id, :tier, :language, :execution_ms, :status, :completed_at)
        ON CONFLICT (submission_id) DO NOTHING`
	_, err := db.NamedExec(query, r)
	return err
}
