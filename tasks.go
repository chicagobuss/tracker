package main

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

type Task struct {
	ID        string          `json:"id"`
	Title     string          `json:"title"`
	Status    string          `json:"status"`
	ClaimedBy string          `json:"claimed_by,omitempty"`
	ClaimedAt *time.Time      `json:"claimed_at,omitempty"`
	Payload   json.RawMessage `json:"payload"`
	Result    json.RawMessage `json:"result,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

func scanTask(row pgx.Row) (*Task, error) {
	var t Task
	var claimedBy *string
	var result *json.RawMessage
	err := row.Scan(&t.ID, &t.Title, &t.Status, &claimedBy, &t.ClaimedAt, &t.Payload, &result, &t.CreatedAt, &t.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if claimedBy != nil {
		t.ClaimedBy = *claimedBy
	}
	if result != nil {
		t.Result = *result
	}
	return &t, nil
}

const taskSelect = `id, title, status, claimed_by, claimed_at, payload, result, created_at, updated_at`

func (s *Store) CreateTask(ctx context.Context, title string, payload json.RawMessage) (*Task, error) {
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	return scanTask(s.db.QueryRow(ctx, `
		insert into tasks (title, payload) values ($1, $2)
		returning `+taskSelect, title, payload))
}

// ClaimNextTask atomically grabs the oldest open task using FOR UPDATE SKIP
// LOCKED, so concurrent agents never claim the same task. Returns ErrNotFound
// when the queue is empty.
func (s *Store) ClaimNextTask(ctx context.Context, worker string) (*Task, error) {
	return scanTask(s.db.QueryRow(ctx, `
		update tasks set status = 'claimed', claimed_by = $1, claimed_at = now(), updated_at = now()
		where id = (
			select id from tasks where status = 'open'
			order by created_at for update skip locked limit 1
		)
		returning `+taskSelect, worker))
}

// CompleteTask sets a terminal status (done/failed) and stores the result.
func (s *Store) CompleteTask(ctx context.Context, id, status string, result json.RawMessage) (*Task, error) {
	if status != "done" && status != "failed" {
		status = "done"
	}
	return scanTask(s.db.QueryRow(ctx, `
		update tasks set status = $2, result = $3, updated_at = now()
		where id::text = $1
		returning `+taskSelect, id, status, result))
}
