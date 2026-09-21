package main

import (
	"log"
	"net/http"

	"async-job-queue/internal/job"
	"async-job-queue/internal/pdf"
	"async-job-queue/internal/text"
)

func main() {
	textGenerator := text.NewGenerator()
	pdfGenerator := pdf.NewGenerator(textGenerator)
	jobService := job.NewService(pdfGenerator)
	jobHandler := job.NewHandler(jobService)

	mux := http.NewServeMux()
	mux.Handle("/job", jobHandler)

	log.Fatal(http.ListenAndServe(":8080", mux))
}
