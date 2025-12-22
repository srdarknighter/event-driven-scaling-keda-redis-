package redisqueue

import (
	"context"
	"encoding/json"
	"judge-worker/internal/job"

	"github.com/redis/go-redis/v9"
)

type Queue struct {
	rdb *redis.Client
	ctx context.Context
}

func New(rdb *redis.Client, ctx context.Context) *Queue {
	return &Queue{rdb: rdb, ctx: ctx}
}

func (q *Queue) Enqueue(job job.Job) error {
	queueName := "free-submissions"
	if job.Tier == "premium" {
		queueName = "premium-submissions"
	}
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}
	return q.rdb.LPush(q.ctx, queueName, data).Err()
}
