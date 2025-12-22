package job

type Job struct {
	SubmissionID string `json:"submission_id"`
	UserID       string `json:"user_id"`
	Language     string `json:"language"`
	Tier         string `json:"tier"`
}
