package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type HealthResponse struct {
	Status bool `json:"status"`
}

func env(key, def string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	return v
}

func mustURL(raw string) *url.URL {
	u, err := url.Parse(raw)
	if err != nil {
		log.Fatalf("invalid url %q: %v", raw, err)
	}
	return u
}

func newReverseProxy(target *url.URL) *httputil.ReverseProxy {
	proxy := httputil.NewSingleHostReverseProxy(target)

	// Preserve original host if needed; for local docker network it's fine to use target host.
	originalDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		originalDirector(req)
		// Ensure we don't accidentally forward Proxy headers
		req.Header.Del("X-Forwarded-Host")
	}

	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("proxy error: %v", err)
		http.Error(w, "bad gateway", http.StatusBadGateway)
	}
	return proxy
}

func main() {
	rand.Seed(time.Now().UnixNano())

	port := env("PORT", "8000")
	monolithURL := mustURL(env("MONOLITH_URL", "http://monolith:8080"))
	moviesURL := mustURL(env("MOVIES_SERVICE_URL", "http://movies-service:8081"))

	gradual := strings.ToLower(env("GRADUAL_MIGRATION", "true")) == "true"
	percentStr := env("MOVIES_MIGRATION_PERCENT", "100")
	percent, err := strconv.Atoi(percentStr)
	if err != nil || percent < 0 || percent > 100 {
		log.Printf("invalid MOVIES_MIGRATION_PERCENT=%q, fallback to 100", percentStr)
		percent = 100
	}

	monolithProxy := newReverseProxy(monolithURL)
	moviesProxy := newReverseProxy(moviesURL)

	mux := http.NewServeMux()

	// Health check
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(HealthResponse{Status: true})
	})

	// Proxy route: /api/users -> monolith
	mux.HandleFunc("/api/users", func(w http.ResponseWriter, r *http.Request) {
		monolithProxy.ServeHTTP(w, r)
	})

	// Proxy route: /api/movies -> gradual migration between monolith and movies-service
	mux.HandleFunc("/api/movies", func(w http.ResponseWriter, r *http.Request) {
		useMoviesService := true
		if gradual {
			n := rand.Intn(100) // 0..99
			useMoviesService = n < percent
		}

		// Simple debug log to verify routing during migration tests
		if useMoviesService {
			log.Printf("route /api/movies -> movies-service (gradual=%v, percent=%d)", gradual, percent)
			moviesProxy.ServeHTTP(w, r)
			return
		}

		log.Printf("route /api/movies -> monolith (gradual=%v, percent=%d)", gradual, percent)
		monolithProxy.ServeHTTP(w, r)
	})

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-context.Background().Done()
		_ = server.Shutdown(context.Background())
	}()

	log.Printf("proxy-service listening on :%s", port)
	log.Printf("monolith=%s movies=%s gradual=%v percent=%d", monolithURL, moviesURL, gradual, percent)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("listen: %v", err)
	}
}
