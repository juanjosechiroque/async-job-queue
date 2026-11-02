package ratelimit

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

func TestLimiterAllowsTenJobCreationsPerMinutePerIP(t *testing.T) {
	limiter := New()
	nextCalls := 0
	handler := limiter.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nextCalls++
		w.WriteHeader(http.StatusAccepted)
	}))

	for range requestsPerMinute {
		recorder := serve(handler, http.MethodPost, "/jobs", "192.0.2.1:1234")
		if recorder.Code != http.StatusAccepted {
			t.Fatalf("request within minute status = %d, want %d", recorder.Code, http.StatusAccepted)
		}
	}

	recorder := serve(handler, http.MethodPost, "/jobs", "192.0.2.1:1234")
	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("request exceeding minute limit status = %d, want %d", recorder.Code, http.StatusTooManyRequests)
	}
	if recorder.Body.String() != "{\"error\":\"rate limit exceeded\"}\n" {
		t.Fatalf("response body = %q, want rate limit error", recorder.Body.String())
	}
	if retryAfter, err := strconv.Atoi(recorder.Header().Get("Retry-After")); err != nil || retryAfter != 6 {
		t.Fatalf("Retry-After = %q, want 6 seconds", recorder.Header().Get("Retry-After"))
	}
	if nextCalls != requestsPerMinute {
		t.Fatalf("next calls = %d, want %d", nextCalls, requestsPerMinute)
	}
}

func TestLimiterAppliesHourlyLimit(t *testing.T) {
	limiter := New()
	start := time.Date(2026, time.September, 22, 12, 0, 0, 0, time.UTC)
	limiter.clients["192.0.2.1"] = client{
		minute:   rate.NewLimiter(rate.Inf, 1),
		hour:     rate.NewLimiter(rate.Every(time.Hour/requestsPerHour), requestsPerHour),
		lastSeen: start,
	}

	for range requestsPerHour {
		allowed, _ := limiter.allow("192.0.2.1", start)
		if !allowed {
			t.Fatal("request within hourly burst was rejected")
		}
	}

	allowed, retryAfter := limiter.allow("192.0.2.1", start)
	if allowed {
		t.Fatal("request exceeding hourly limit was allowed")
	}
	if retryAfter != 36*time.Second {
		t.Fatalf("retry after = %s, want 36s", retryAfter)
	}
}

func TestLimiterOnlyLimitsJobCreationAndSeparatesIPs(t *testing.T) {
	limiter := New()
	nextCalls := 0
	handler := limiter.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nextCalls++
		w.WriteHeader(http.StatusOK)
	}))

	for range requestsPerMinute {
		serve(handler, http.MethodPost, "/jobs", "192.0.2.1:1234")
	}
	if recorder := serve(handler, http.MethodPost, "/jobs", "192.0.2.2:1234"); recorder.Code != http.StatusOK {
		t.Fatalf("other IP status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if recorder := serve(handler, http.MethodGet, "/jobs/example", "192.0.2.1:1234"); recorder.Code != http.StatusOK {
		t.Fatalf("job read status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if nextCalls != requestsPerMinute+2 {
		t.Fatalf("next calls = %d, want %d", nextCalls, requestsPerMinute+2)
	}
}

func TestLimiterRemovesInactiveClients(t *testing.T) {
	limiter := New()
	start := time.Date(2026, time.September, 22, 12, 0, 0, 0, time.UTC)
	if allowed, _ := limiter.allow("192.0.2.1", start); !allowed {
		t.Fatal("initial request was rejected")
	}
	if allowed, _ := limiter.allow("192.0.2.2", start.Add(time.Hour+time.Minute)); !allowed {
		t.Fatal("later request was rejected")
	}
	if _, exists := limiter.clients["192.0.2.1"]; exists {
		t.Fatal("inactive client was not removed")
	}
}

func serve(handler http.Handler, method, target, remoteAddr string) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(method, target, nil)
	request.RemoteAddr = remoteAddr
	handler.ServeHTTP(recorder, request)
	return recorder
}
