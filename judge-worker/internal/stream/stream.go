package stream

import (
	"context"
	"strconv"
	"strings"
	"time"

	"judge-worker/internal/job"

	"github.com/redis/go-redis/v9"
)

func EnsureConsumerGroup(ctx context.Context, rdb *redis.Client, stream, group string) error {
	err := rdb.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

func PublishResult(ctx context.Context, rdb *redis.Client, result *job.ResultEvent) error {
	_, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "submission",
		Values: map[string]interface{}{
			"submission_id": result.SubmissionID,
			"user_id":       result.UserID,
			"tier":          result.Tier,
			"language":      result.Language,
			"execution_ms":  result.ExecutionMs,
			"status":        result.Status,
			"completed_at":  result.CompletedAt.Format(time.RFC3339),
		},
		ID: "*",
	}).Result()
	return err
}

func MapToResultEvent(values map[string]interface{}) (*job.ResultEvent, error) {
	getStr := func(k string) string {
		if v, ok := values[k].(string); ok {
			return v
		}
		return ""
	}

	execMs, _ := strconv.Atoi(getStr("execution_ms"))
	completedAt, err := time.Parse(time.RFC3339, getStr("completed_at"))
	if err != nil {
		completedAt = time.Now()
	}

	return &job.ResultEvent{
		SubmissionID: getStr("submission_id"),
		UserID:       getStr("user_id"),
		Tier:         getStr("tier"),
		Language:     getStr("language"),
		ExecutionMs:  execMs,
		Status:       getStr("status"),
		CompletedAt:  completedAt,
	}, nil
}
