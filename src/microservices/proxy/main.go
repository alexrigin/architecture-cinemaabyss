package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

var monolithHost = getEnv("MONOLITH_URL", "")
var moviesServiceHost = getEnv("MOVIES_SERVICE_URL", "")
var eventsServiceHost = getEnv("EVENTS_SERVICE_URL", "")

func main() {

	http.HandleFunc("/health", func(res http.ResponseWriter, req *http.Request) {
		res.Header().Set("Content-Type", "application/json")
		json.NewEncoder(res).Encode(map[string]bool{"status": true})
	})

	http.HandleFunc("/", func(res http.ResponseWriter, req *http.Request) {
		host := req.Host
		path := req.URL.Path
		//qs := req.URL.Query()
		log.Printf("incoming request: %s %", host, req.URL.String())

		targetURL := lookupTargetURL(path)
		if targetURL == "" {
			http.Error(res, "Not Found", 404)
			return
		}
		proxy(targetURL, res, req)
	})

	// Start server
	srv := &http.Server{
		Addr:    ":" + getEnv("PORT", ":8000"),
		Handler: nil,
	}

	// Start the server in a goroutine
	go func() {
		log.Printf("Server starting on %s\n", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Failed to start server: %v\n", err)
		}
	}()

	// Wait for interrupt signal to gracefully shut down the server
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down server...")

	// Create a deadline for server shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v\n", err)
	}

	log.Println("Server exited properly")
}

func lookupTargetURL(path string) string {

	targetURL := ""
	normalizedPath := strings.Trim(path, "/")
	parts := strings.Split(normalizedPath, "/")

	if len(parts) >= 2 {
		if parts[0] == "api" {

			if parts[1] == "movies" {
				targetURL = moviesServiceHost + path
			}

			if parts[1] == "users" {
				targetURL = monolithHost + path
			}
		}
	}

	return targetURL
}

func proxy(targetURL string, res http.ResponseWriter, req *http.Request) {
	target, err := url.Parse(targetURL)
	if err != nil {
		http.Error(res, "Invalid URL", 500)
		return
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Director = func(request *http.Request) {
		request.URL.Scheme = target.Scheme
		request.URL.Host = target.Host
		request.URL.Path = target.Path
	}
	log.Printf("Forwarding request to %v", target)
	proxy.ServeHTTP(res, req)
}

// getEnv gets an environment variable or returns a default value
func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}
