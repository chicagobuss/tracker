package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func newTask(t *testing.T, s *Store, title string) *Task {
	t.Helper()
	task, err := s.CreateTask(context.Background(), title, json.RawMessage(`{"detail":"x"}`), "tester")
	if err != nil {
		t.Fatalf("create task %q: %v", title, err)
	}
	if task.Status != "open" {
		t.Fatalf("new task status = %q, want open", task.Status)
	}
	return task
}

// The core queue guarantee: with N workers racing for N tasks, FOR UPDATE SKIP
// LOCKED must hand each task to exactly one worker.
func TestClaimNextTask_NoDoubleClaim(t *testing.T) {
	s := testStore(t)

	const tasks = 12
	for i := 0; i < tasks; i++ {
		newTask(t, s, fmt.Sprintf("task-%d", i))
	}

	const workers = 12
	var wg sync.WaitGroup
	var mu sync.Mutex
	claimed := map[string]string{} // task id -> worker

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			worker := fmt.Sprintf("worker-%d", i)
			task, err := s.ClaimNextTask(context.Background(), worker, time.Hour)
			if errors.Is(err, ErrNotFound) {
				return // nothing left; fine
			}
			if err != nil {
				t.Errorf("%s: claim: %v", worker, err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if prev, dup := claimed[task.ID]; dup {
				t.Errorf("task %s claimed twice: by %s and %s", task.ID, prev, worker)
			}
			claimed[task.ID] = worker
		}(i)
	}
	wg.Wait()

	if len(claimed) != tasks {
		t.Errorf("claimed %d distinct tasks, want %d", len(claimed), tasks)
	}
}

func TestClaimNextTask_EmptyQueue(t *testing.T) {
	s := testStore(t)
	if _, err := s.ClaimNextTask(context.Background(), "worker", time.Hour); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestClaimNextTask_MarksClaimant(t *testing.T) {
	s := testStore(t)
	newTask(t, s, "solo")

	got, err := s.ClaimNextTask(context.Background(), "worker-1", time.Hour)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if got.Status != "claimed" {
		t.Errorf("status = %q, want claimed", got.Status)
	}
	if got.ClaimedBy != "worker-1" {
		t.Errorf("claimed_by = %q, want worker-1", got.ClaimedBy)
	}
	if got.Attempts != 1 {
		t.Errorf("attempts = %d, want 1", got.Attempts)
	}
}

// A live claim is exclusive: another worker can't steal it.
func TestClaimTask_LiveClaimNotClaimable(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	task := newTask(t, s, "held")

	if _, err := s.ClaimTask(ctx, task.ID, "worker-1", time.Hour); err != nil {
		t.Fatalf("first claim: %v", err)
	}
	_, err := s.ClaimTask(ctx, task.ID, "worker-2", time.Hour)
	if !errors.Is(err, ErrNotClaimable) {
		t.Fatalf("err = %v, want ErrNotClaimable", err)
	}
}

// A worker that dies mid-task must not strand it: once the claim TTL lapses, the
// task is claimable again and the attempt count reflects the retry.
func TestClaimTask_ExpiredClaimIsReclaimable(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	task := newTask(t, s, "abandoned")

	if _, err := s.ClaimTask(ctx, task.ID, "worker-1", time.Hour); err != nil {
		t.Fatalf("first claim: %v", err)
	}

	// A zero TTL makes the existing claim instantly stale, standing in for a
	// worker that died an hour ago.
	got, err := s.ClaimTask(ctx, task.ID, "worker-2", 0)
	if err != nil {
		t.Fatalf("reclaim expired: %v", err)
	}
	if got.ClaimedBy != "worker-2" {
		t.Errorf("claimed_by = %q, want worker-2", got.ClaimedBy)
	}
	if got.Attempts != 2 {
		t.Errorf("attempts = %d, want 2", got.Attempts)
	}
}

func TestClaimNextTask_SkipsLiveClaims(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	first := newTask(t, s, "first")
	second := newTask(t, s, "second")

	if _, err := s.ClaimTask(ctx, first.ID, "worker-1", time.Hour); err != nil {
		t.Fatalf("claim first: %v", err)
	}

	// The next claimer must skip the live claim and take the open task.
	got, err := s.ClaimNextTask(ctx, "worker-2", time.Hour)
	if err != nil {
		t.Fatalf("claim next: %v", err)
	}
	if got.ID != second.ID {
		t.Errorf("claimed %q, want the open task %q", got.Title, second.Title)
	}
}

func TestClaimTask_MissingTask(t *testing.T) {
	s := testStore(t)
	_, err := s.ClaimTask(context.Background(), "00000000-0000-0000-0000-000000000000", "worker", time.Hour)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestCompleteTask_Success(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	task := newTask(t, s, "completable")

	if _, err := s.ClaimTask(ctx, task.ID, "worker-1", time.Hour); err != nil {
		t.Fatalf("claim: %v", err)
	}
	done, err := s.CompleteTask(ctx, task.ID, "done", json.RawMessage(`{"ok":true}`), "worker-1")
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if done.Status != "done" {
		t.Errorf("status = %q, want done", done.Status)
	}
	// Compare parsed, not raw: Postgres stores jsonb normalized, so the exact
	// byte spacing coming back is not ours to predict.
	var result map[string]any
	if err := json.Unmarshal(done.Result, &result); err != nil {
		t.Fatalf("result is not valid json (%s): %v", done.Result, err)
	}
	if result["ok"] != true {
		t.Errorf("result = %s, want {\"ok\":true}", done.Result)
	}
}

// Only the current claimant may complete a task — otherwise any agent could
// close out work it never did.
func TestCompleteTask_ClaimantOnly(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	task := newTask(t, s, "not-yours")

	if _, err := s.ClaimTask(ctx, task.ID, "worker-1", time.Hour); err != nil {
		t.Fatalf("claim: %v", err)
	}
	_, err := s.CompleteTask(ctx, task.ID, "done", nil, "worker-2")
	if !errors.Is(err, ErrNotClaimant) {
		t.Fatalf("err = %v, want ErrNotClaimant", err)
	}
}

func TestCompleteTask_UnclaimedTask(t *testing.T) {
	s := testStore(t)
	task := newTask(t, s, "never-claimed")

	_, err := s.CompleteTask(context.Background(), task.ID, "done", nil, "worker-1")
	if !errors.Is(err, ErrNotClaimant) {
		t.Fatalf("err = %v, want ErrNotClaimant", err)
	}
}

func TestCompleteTask_RejectsBadStatus(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	task := newTask(t, s, "bad-status")

	if _, err := s.ClaimTask(ctx, task.ID, "worker-1", time.Hour); err != nil {
		t.Fatalf("claim: %v", err)
	}
	_, err := s.CompleteTask(ctx, task.ID, "sorta-done", nil, "worker-1")
	if !errors.Is(err, ErrBadTaskStatus) {
		t.Fatalf("err = %v, want ErrBadTaskStatus", err)
	}
}

func TestListTasks_FilterByStatus(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	newTask(t, s, "open-1")
	claimMe := newTask(t, s, "claim-me")
	if _, err := s.ClaimTask(ctx, claimMe.ID, "worker-1", time.Hour); err != nil {
		t.Fatalf("claim: %v", err)
	}

	open, total, err := s.ListTasks(ctx, "open", 50, 0)
	if err != nil {
		t.Fatalf("list open: %v", err)
	}
	if len(open) != 1 || total != 1 || open[0].Title != "open-1" {
		t.Errorf("open tasks = %d (total %d), want just open-1", len(open), total)
	}

	all, total, err := s.ListTasks(ctx, "", 50, 0)
	if err != nil {
		t.Fatalf("list all: %v", err)
	}
	if len(all) != 2 || total != 2 {
		t.Errorf("all tasks = %d (total %d), want 2", len(all), total)
	}
}
