package postgres

import (
	"context"
	"fmt"
	"io/fs"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// isolatedDatabaseURL gives each migration test its own search path. The
// application still creates its regular, unqualified table names.
func isolatedDatabaseURL(t *testing.T) string {
	t.Helper()
	base := integrationDatabaseURL(t)
	admin, err := pgxpool.New(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	schemaName := fmt.Sprintf("test_migration_%d", time.Now().UnixNano())
	identifier := pgx.Identifier{schemaName}.Sanitize()
	if _, err := admin.Exec(context.Background(), "CREATE SCHEMA "+identifier); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(context.Background(), "DROP SCHEMA "+identifier+" CASCADE"); err != nil {
			t.Errorf("drop test schema: %v", err)
		}
	})
	parsed, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("search_path", schemaName)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func TestMigrationsEmptyAndRepeated(t *testing.T) {
	databaseURL := isolatedDatabaseURL(t)
	first := newIntegrationStore(t, databaseURL)
	var firstApplied time.Time
	if err := first.pool.QueryRow(context.Background(), "SELECT applied_at FROM schema_migrations WHERE version = 1").Scan(&firstApplied); err != nil {
		t.Fatal(err)
	}
	second := newIntegrationStore(t, databaseURL)
	var secondApplied time.Time
	if err := second.pool.QueryRow(context.Background(), "SELECT applied_at FROM schema_migrations WHERE version = 1").Scan(&secondApplied); err != nil {
		t.Fatal(err)
	}
	if !firstApplied.Equal(secondApplied) {
		t.Fatal("repeated migration changed applied_at")
	}
	var count int
	if err := second.pool.QueryRow(context.Background(), "SELECT count(*) FROM schema_migrations").Scan(&count); err != nil || count != migrationCount(t) {
		t.Fatalf("migration count = %d, error = %v", count, err)
	}
	if err := second.pool.QueryRow(context.Background(), "SELECT count(*) FROM jobs").Scan(&count); err != nil {
		t.Fatalf("jobs table absent: %v", err)
	}
}

func TestMigrationsPreexistingJobsTable(t *testing.T) {
	databaseURL := isolatedDatabaseURL(t)
	pool, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(context.Background(), `CREATE TABLE jobs (
		id VARCHAR(10) PRIMARY KEY, lines INTEGER NOT NULL,
		status TEXT NOT NULL CHECK (status IN ('queued', 'processing', 'completed', 'failed')),
		file_path TEXT NOT NULL DEFAULT '', error TEXT NOT NULL DEFAULT '',
		created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL
	)`); err != nil {
		t.Fatal(err)
	}
	store := newIntegrationStore(t, databaseURL)
	var count int
	if err := store.pool.QueryRow(context.Background(), "SELECT count(*) FROM schema_migrations").Scan(&count); err != nil || count != migrationCount(t) {
		t.Fatalf("migration count = %d, error = %v", count, err)
	}
}

func TestMigrationsConcurrentStarts(t *testing.T) {
	databaseURL := isolatedDatabaseURL(t)
	const instances = 2
	start := make(chan struct{})
	results := make(chan error, instances)
	var wg sync.WaitGroup
	for range instances {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			store, err := New(context.Background(), databaseURL)
			if err == nil {
				store.Close()
			}
			results <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Errorf("concurrent New: %v", err)
		}
	}
	store := newIntegrationStore(t, databaseURL)
	var count int
	if err := store.pool.QueryRow(context.Background(), "SELECT count(*) FROM schema_migrations").Scan(&count); err != nil || count != migrationCount(t) {
		t.Fatalf("migration count = %d, error = %v", count, err)
	}
}

func migrationCount(t *testing.T) int {
	t.Helper()
	paths, err := fs.Glob(migrations, "migrations/*.sql")
	if err != nil {
		t.Fatal(err)
	}
	return len(paths)
}
