package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"async-job-queue/internal/jobs"
	"async-job-queue/internal/pdf"
	"async-job-queue/internal/text"
)

func main() {
	const storageDir = "storage"
	if err := os.MkdirAll(storageDir, 0o755); err != nil {
		log.Fatalf("create storage directory: %v", err)
	}

	textGenerator := text.NewGenerator()
	pdfGenerator := pdf.NewGenerator(textGenerator)
	jobService := jobs.NewService(pdfGenerator, storageDir)
	jobHandler := jobs.NewHandler(jobService)

	mux := http.NewServeMux()
	mux.Handle("/jobs", jobHandler)
	mux.Handle("/jobs/", jobHandler)

	server := &http.Server{Addr: ":8080", Handler: mux}
	serverErrors := make(chan error, 1)
	go func() {
		serverErrors <- server.ListenAndServe()
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)

	select {
	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP server failed: %v", err)
		}
	case received := <-signals:
		log.Printf("received %s; beginning graceful shutdown", received)
		jobService.StopAccepting()
		shutdown, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			log.Printf("HTTP shutdown error: %v", err)
		}
		if err := jobService.Wait(shutdown); err != nil {
			log.Printf("background job shutdown timeout: %v", err)
		}
	}
}
