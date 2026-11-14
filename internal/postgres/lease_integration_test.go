package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/juanjosechiroque/async-job-queue/internal/jobs"
)

func TestExpiredClaimRetriesAndEventuallyDies(t *testing.T) {
	store := newIntegrationStore(t, isolatedDatabaseURL(t))
	now := time.Now().UTC()
	if created, err := store.CreateQueued(context.Background(), jobs.Job{
		ID: "expired001", Lines: 1, Status: jobs.StatusQueued, CreatedAt: now, UpdatedAt: now,
	}, 100); err != nil || !created {
		t.Fatalf("create = (%t, %v)", created, err)
	}
	first, claimed, err := store.ClaimQueued(context.Background())
	if err != nil || !claimed {
		t.Fatalf("claim = (%t, %v)", claimed, err)
	}
	if _, err := store.pool.Exec(context.Background(), "UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = 'expired001'"); err != nil {
		t.Fatal(err)
	}
	transitions, err := store.ReapExpired(context.Background())
	if err != nil || len(transitions) != 1 || transitions[0] != (jobs.JobTransition{ID: "expired001", Status: jobs.StatusQueued}) {
		t.Fatalf("reaped = (%v, %v)", transitions, err)
	}
	job, err := store.Get(context.Background(), "expired001")
	if err != nil || job.Status != jobs.StatusQueued || job.Error != "lease expired" {
		t.Fatalf("expired job = (%+v, %v)", job, err)
	}
	if err := store.MarkCompleted(context.Background(), job.ID, first.ClaimVersion, "late.pdf"); !errors.Is(err, jobs.ErrJobNotFound) {
		t.Fatalf("late completion error = %v", err)
	}
	if _, err := store.pool.Exec(context.Background(), "UPDATE jobs SET available_at = now() WHERE id = $1", job.ID); err != nil {
		t.Fatal(err)
	}
	second, claimed, err := store.ClaimQueued(context.Background())
	if err != nil || !claimed || second.ClaimVersion <= first.ClaimVersion || second.Attempts != 2 {
		t.Fatalf("second claim = (%+v, %t, %v)", second, claimed, err)
	}
	if active, err := store.ExtendLease(context.Background(), second.ID, first.ClaimVersion); err != nil || active {
		t.Fatalf("stale heartbeat = (%t, %v)", active, err)
	}
	if err := store.MarkCompleted(context.Background(), second.ID, first.ClaimVersion, "late.pdf"); !errors.Is(err, jobs.ErrJobNotFound) {
		t.Fatalf("stale completion error = %v", err)
	}
	if status, err := store.FailAttempt(context.Background(), second.ID, second.ClaimVersion, "second failed"); err != nil || status != jobs.StatusQueued {
		t.Fatalf("second failure = (%q, %v)", status, err)
	}
	if _, err := store.pool.Exec(context.Background(), "UPDATE jobs SET available_at = now() WHERE id = $1", job.ID); err != nil {
		t.Fatal(err)
	}
	third, claimed, err := store.ClaimQueued(context.Background())
	if err != nil || !claimed || third.Attempts != 3 {
		t.Fatalf("third claim = (%+v, %t, %v)", third, claimed, err)
	}
	if _, err := store.pool.Exec(context.Background(), "UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1", job.ID); err != nil {
		t.Fatal(err)
	}
	transitions, err = store.ReapExpired(context.Background())
	if err != nil || len(transitions) != 1 || transitions[0].Status != jobs.StatusDead {
		t.Fatalf("exhausted retry = (%v, %v)", transitions, err)
	}
}

func TestHeartbeatKeepsSlowJobAlive(t *testing.T) {
	store := newIntegrationStore(t, isolatedDatabaseURL(t))
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	service := jobs.NewService(blockingGenerator{started: started, release: release}, t.TempDir(), 1, store)
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
		stopAndWait(t, service, 3*time.Second)
	})
	created, err := service.CreateJob(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("slow generator did not start")
	}
	before, err := store.Get(context.Background(), created.ID)
	if err != nil || before.LeaseExpiresAt == nil {
		t.Fatalf("claimed lease = (%+v, %v)", before, err)
	}
	deadline := time.Now().Add(13 * time.Second)
	for time.Now().Before(deadline) {
		current, err := store.Get(context.Background(), created.ID)
		if err != nil {
			t.Fatal(err)
		}
		if current.LeaseExpiresAt != nil && current.LeaseExpiresAt.After(*before.LeaseExpiresAt) {
			ids, err := store.ReapExpired(context.Background())
			if err != nil || len(ids) != 0 {
				t.Fatalf("reaper touched live job: (%v, %v)", ids, err)
			}
			close(release)
			waitForCompletedJob(t, service, created.ID)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("heartbeat did not extend the slow job lease")
}

func TestNewServiceRecoversClaimAbandonedByOtherInstance(t *testing.T) {
	databaseURL := isolatedDatabaseURL(t)
	storeA := newIntegrationStore(t, databaseURL)
	storeB := newIntegrationStore(t, databaseURL)
	now := time.Now().UTC()
	if created, err := storeA.CreateQueued(context.Background(), jobs.Job{
		ID: "abandoned1", Lines: 1, Status: jobs.StatusQueued, CreatedAt: now, UpdatedAt: now,
	}, 100); err != nil || !created {
		t.Fatalf("create = (%t, %v)", created, err)
	}
	first, claimed, err := storeA.ClaimQueued(context.Background())
	if err != nil || !claimed {
		t.Fatalf("first claim = (%+v, %t, %v)", first, claimed, err)
	}
	if _, err := storeA.pool.Exec(context.Background(), "UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1", first.ID); err != nil {
		t.Fatal(err)
	}
	serviceB := jobs.NewService(immediateGenerator{}, t.TempDir(), 1, storeB)
	t.Cleanup(func() { stopAndWait(t, serviceB, 3*time.Second) })
	deadline := time.Now().Add(18 * time.Second)
	for time.Now().Before(deadline) {
		job, err := serviceB.GetJob(context.Background(), first.ID)
		if err != nil {
			t.Fatal(err)
		}
		if job.Status == jobs.StatusCompleted {
			if job.Attempts != 2 || job.ClaimVersion <= first.ClaimVersion {
				t.Fatalf("recovered job = %+v", job)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("another service did not recover abandoned claim")
}

type failingGenerator struct{}

func (failingGenerator) Generate(int) ([]byte, error) { return nil, errors.New("generation secret") }

func TestGenerationFailureIsDeadInPostgres(t *testing.T) {
	store := newIntegrationStore(t, isolatedDatabaseURL(t))
	service := jobs.NewService(failingGenerator{}, t.TempDir(), 1, store)
	t.Cleanup(func() { stopAndWait(t, service, time.Second) })
	job, err := service.CreateJob(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	dead := waitForTerminalJob(t, service, job.ID)
	if dead.Status != jobs.StatusDead || dead.Error != "generation secret" || dead.Attempts != jobs.MaxAttempts {
		t.Fatalf("failed generation = %+v", dead)
	}
	handler := jobs.NewHandler(service)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/jobs/"+job.ID, nil))
	if response.Code != http.StatusOK || strings.Contains(response.Body.String(), "generation secret") || !strings.Contains(response.Body.String(), `"status":"dead"`) {
		t.Fatalf("public status = %d %s", response.Code, response.Body.String())
	}
}

func TestHTTPContractWithPostgres(t *testing.T) {
	store := newIntegrationStore(t, isolatedDatabaseURL(t))
	service := jobs.NewService(immediateGenerator{}, t.TempDir(), 1, store)
	t.Cleanup(func() { stopAndWait(t, service, time.Second) })
	handler := jobs.NewHandler(service)
	request := func(method, path, body string) *httptest.ResponseRecorder {
		r := httptest.NewRecorder()
		handler.ServeHTTP(r, httptest.NewRequest(method, path, strings.NewReader(body)))
		return r
	}
	for _, tc := range []struct {
		method, body string
		want         int
	}{
		{http.MethodPost, `{"lines":`, http.StatusBadRequest},
		{http.MethodPost, `{"lines":0}`, http.StatusBadRequest},
		{http.MethodPost, `{"lines":250001}`, http.StatusBadRequest},
		{http.MethodGet, "", http.StatusMethodNotAllowed},
	} {
		if got := request(tc.method, "/jobs", tc.body).Code; got != tc.want {
			t.Fatalf("%s /jobs %q = %d, want %d", tc.method, tc.body, got, tc.want)
		}
	}
	created := request(http.MethodPost, "/jobs", `{"lines":1}`)
	if created.Code != http.StatusAccepted {
		t.Fatalf("valid create = %d: %s", created.Code, created.Body.String())
	}
	var response struct {
		JobID string `json:"jobId"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	waitForCompletedJob(t, service, response.JobID)
	file := request(http.MethodGet, "/jobs/"+response.JobID+"/file", "")
	if file.Code != http.StatusOK || file.Header().Get("Content-Type") != "application/pdf" || file.Header().Get("Content-Disposition") == "" || !bytes.HasPrefix(file.Body.Bytes(), []byte("%PDF")) {
		t.Fatalf("PDF response = %d, headers %v, prefix %q", file.Code, file.Header(), file.Body.String())
	}
}
