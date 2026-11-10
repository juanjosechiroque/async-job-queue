package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/juanjosechiroque/async-job-queue/internal/jobs"
	"github.com/juanjosechiroque/async-job-queue/internal/pdf"
	jobpostgres "github.com/juanjosechiroque/async-job-queue/internal/postgres"
	"github.com/juanjosechiroque/async-job-queue/internal/ratelimit"
	"github.com/juanjosechiroque/async-job-queue/internal/text"
)

func main() {
	const storageDir = "storage"
	const workers = 4
	if err := os.MkdirAll(storageDir, 0o755); err != nil {
		slog.Error("create storage directory", slog.Any("error", err))
		os.Exit(1)
	}
	databaseURL, ok := os.LookupEnv("DATABASE_URL")
	if !ok || databaseURL == "" {
		slog.Error("DATABASE_URL must be set")
		os.Exit(1)
	}
	jobStore, err := jobpostgres.New(context.Background(), databaseURL)
	if err != nil {
		slog.Error("connect to Postgres", slog.Any("error", err))
		os.Exit(1)
	}
	defer jobStore.Close()

	textGenerator := text.NewGenerator()
	pdfGenerator := pdf.NewGenerator(textGenerator)
	jobService := jobs.NewService(pdfGenerator, storageDir, workers, jobStore)
	jobHandler := jobs.NewHandler(jobService)

	mux := http.NewServeMux()
	mux.Handle("/jobs", jobHandler)
	mux.Handle("/jobs/", jobHandler)
	limiter := ratelimit.New()

	server := &http.Server{Addr: ":8080", Handler: limiter.Middleware(mux), ReadHeaderTimeout: 5 * time.Second}
	pprofServer := &http.Server{Addr: "127.0.0.1:6060", Handler: http.DefaultServeMux, ReadHeaderTimeout: 5 * time.Second}
	serverErrors := make(chan error, 2)
	go func() {
		serverErrors <- server.ListenAndServe()
	}()
	go func() {
		serverErrors <- pprofServer.ListenAndServe()
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)

	select {
	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			slog.Error("HTTP server failed", slog.Any("error", err))
			os.Exit(1)
		}
	case received := <-signals:
		slog.Info("received signal; beginning graceful shutdown", slog.String("signal", received.String()))
		jobService.StopAccepting()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			slog.Error("HTTP shutdown error", slog.Any("error", err))
		}
		if err := pprofServer.Shutdown(shutdown); err != nil {
			slog.Error("pprof shutdown error", slog.Any("error", err))
		}
		if err := jobService.Wait(shutdown); err != nil {
			slog.Error("background job shutdown timeout", slog.Any("error", err))
		}
	}
}
