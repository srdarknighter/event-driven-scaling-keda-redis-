package postgres

import (
	"judge-worker/internal/job"

	"github.com/jmoiron/sqlx"
)

func InsertResultEvent(db *sqlx.DB, r *job.ResultEvent) error {
	_, err := db.NamedExec(`
		INSERT INTO submissions
		    (submission_id, user_id, tier, language, execution_ms, status, completed_at)
		VALUES
		    (:submission_id, :user_id, :tier, :language, :execution_ms, :status, :completed_at)
		ON CONFLICT (submission_id) DO NOTHING`,
		r,
	)
	return err
}
