// Package ratelimit applies per-client request limits to HTTP handlers.
package ratelimit

import (
	"math"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

const (
	requestsPerMinute = 10
	requestsPerHour   = 100
)

type client struct {
	minute   *rate.Limiter
	hour     *rate.Limiter
	lastSeen time.Time
}

// Limiter limits POST /jobs requests independently for each source IP address.
// Limits are intentionally process-local because job state is process-local too.
type Limiter struct {
	mu          sync.Mutex
	clients     map[string]client
	lastCleanup time.Time
}

func New() *Limiter {
	return &Limiter{clients: make(map[string]client)}
}

func (l *Limiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/jobs" {
			next.ServeHTTP(w, r)
			return
		}

		allowed, retryAfter := l.allow(clientIP(r.RemoteAddr), time.Now())
		if !allowed {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.Header().Set("Retry-After", strconv.Itoa(retryAfterSeconds(retryAfter)))
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte("{\"error\":\"rate limit exceeded\"}\n"))
			return
		}

		next.ServeHTTP(w, r)
	})
}

func (l *Limiter) allow(ip string, now time.Time) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.removeInactiveClients(now)
	current, exists := l.clients[ip]
	if !exists {
		current = client{
			minute: rate.NewLimiter(rate.Every(time.Minute/requestsPerMinute), requestsPerMinute),
			hour:   rate.NewLimiter(rate.Every(time.Hour/requestsPerHour), requestsPerHour),
		}
	}
	current.lastSeen = now
	l.clients[ip] = current

	retryAfter := max(
		delayUntilToken(current.minute, now),
		delayUntilToken(current.hour, now),
	)
	if retryAfter > 0 {
		return false, retryAfter
	}

	// Both availability checks and consumes run while l.mu is held, so the two
	// limiters remain atomic as a pair for a client.
	current.minute.AllowN(now, 1)
	current.hour.AllowN(now, 1)
	return true, 0
}

func (l *Limiter) removeInactiveClients(now time.Time) {
	if !l.lastCleanup.IsZero() && now.Sub(l.lastCleanup) < time.Minute {
		return
	}
	for ip, current := range l.clients {
		if current.lastSeen.Before(now.Add(-time.Hour)) {
			delete(l.clients, ip)
		}
	}
	l.lastCleanup = now
}

func delayUntilToken(limiter *rate.Limiter, now time.Time) time.Duration {
	tokens := limiter.TokensAt(now)
	if tokens >= 1 {
		return 0
	}
	return time.Duration(math.Ceil((1 - tokens) / float64(limiter.Limit()) * float64(time.Second)))
}

func clientIP(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err == nil {
		return host
	}
	if net.ParseIP(remoteAddr) != nil {
		return remoteAddr
	}
	return "unknown"
}

func retryAfterSeconds(duration time.Duration) int {
	seconds := int(math.Ceil(duration.Seconds()))
	if seconds < 1 {
		return 1
	}
	return seconds
}
