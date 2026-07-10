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
	Attempts  int             `json:"attempts"`
	Payload   json.RawMessage `json:"payload"`
	Result    json.RawMessage `json:"result,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

func scanTask(row pgx.Row) (*Task, error) {
	var t Task
	var claimedBy *string
	var result *json.RawMessage
	err := row.Scan(&t.ID, &t.Title, &t.Status, &claimedBy, &t.ClaimedAt, &t.Attempts, &t.Payload, &result, &t.CreatedAt, &t.UpdatedAt)
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

const taskSelect = `id, title, status, claimed_by, claimed_at, attempts, payload, result, created_at, updated_at`

func (s *Store) CreateTask(ctx context.Context, title string, payload json.RawMessage, by string) (*Task, error) {
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	t, err := scanTask(s.db.QueryRow(ctx, `
		insert into tasks (title, payload) values ($1, $2)
		returning `+taskSelect, title, payload))
	if err == nil {
		s.touchActor(ctx, by)
	}
	return t, err
}

// ClaimNextTask atomically grabs the oldest claimable task using FOR UPDATE
// SKIP LOCKED, so concurrent agents never claim the same task. A 'claimed' task
// whose claim is older than ttl is claimable again (its worker is presumed
// dead), like an expired doc lease. Returns ErrNotFound when nothing is claimable.
func (s *Store) ClaimNextTask(ctx context.Context, worker string, ttl time.Duration) (*Task, error) {
	t, err := scanTask(s.db.QueryRow(ctx, `
		update tasks set status = 'claimed', claimed_by = $1, claimed_at = now(),
			attempts = attempts + 1, updated_at = now()
		where id = (
			select id from tasks
			where status = 'open'
			   or (status = 'claimed' and claimed_at < now() - make_interval(secs => $2))
			order by created_at for update skip locked limit 1
		)
		returning `+taskSelect, worker, ttl.Seconds()))
	if err == nil {
		s.touchActor(ctx, worker)
	}
	return t, err
}

// CompleteTask sets a terminal status (done/failed) and stores the result. The
// caller must be the current claimant of a claimed task.
func (s *Store) CompleteTask(ctx context.Context, id, status string, result json.RawMessage, by string) (*Task, error) {
	if status != "done" && status != "failed" {
		return nil, ErrBadTaskStatus
	}
	t, err := scanTask(s.db.QueryRow(ctx, `
		update tasks set status = $2, result = $3, updated_at = now()
		where id::text = $1 and status = 'claimed' and claimed_by = $4
		returning `+taskSelect, id, status, result, by))
	if errors.Is(err, ErrNotFound) {
		// No row updated: distinguish a missing task from one the caller doesn't hold.
		if _, gerr := s.GetTask(ctx, id); gerr != nil {
			return nil, gerr
		}
		return nil, ErrNotClaimant
	}
	if err == nil {
		s.touchActor(ctx, by)
	}
	return t, err
}

func (s *Store) ListTasks(ctx context.Context, status string, limit, offset int) ([]Task, int, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	if offset < 0 {
		offset = 0
	}

	filter := `where ($1 = '' or status = $1)`
	var total int
	if err := s.db.QueryRow(ctx, `select count(*) from tasks `+filter, status).Scan(&total); err != nil {
		return nil, 0, err
	}

	rows, err := s.db.Query(ctx, `select `+taskSelect+` from tasks `+filter+` order by created_at desc limit $2 offset $3`, status, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	out := []Task{}
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, *t)
	}
	return out, total, rows.Err()
}

func (s *Store) GetTask(ctx context.Context, id string) (*Task, error) {
	return scanTask(s.db.QueryRow(ctx, `select `+taskSelect+` from tasks where id::text = $1`, id))
}
