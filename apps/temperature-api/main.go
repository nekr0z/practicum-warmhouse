package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	minTemperature = 18.0
	maxTemperature = 28.0
)

// TemperatureResponse represents the temperature reading returned by the API
type TemperatureResponse struct {
	Value       float64   `json:"value"`
	Unit        string    `json:"unit"`
	Timestamp   time.Time `json:"timestamp"`
	Location    string    `json:"location"`
	Status      string    `json:"status"`
	SensorID    string    `json:"sensor_id"`
	SensorType  string    `json:"sensor_type"`
	Description string    `json:"description"`
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handleHealth)
	mux.HandleFunc("GET /temperature", handleTemperature)
	mux.HandleFunc("GET /temperature/{sensorID}", handleTemperatureByID)

	// Start server
	srv := &http.Server{
		Addr:              getEnv("PORT", ":8081"),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
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

// handleHealth handles GET /health
func handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]string{"status": "ok"})
}

// handleTemperature handles GET /temperature?location=&sensorId=
func handleTemperature(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	writeJSON(w, newReading(query.Get("location"), query.Get("sensorId")))
}

// handleTemperatureByID handles GET /temperature/{sensorID}
func handleTemperatureByID(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, newReading("", r.PathValue("sensorID")))
}

// newReading creates a random temperature reading for a location or sensor ID
func newReading(location, sensorID string) TemperatureResponse {
	// If no location is provided, use a default based on sensor ID
	if location == "" {
		switch sensorID {
		case "1":
			location = "Living Room"
		case "2":
			location = "Bedroom"
		case "3":
			location = "Kitchen"
		default:
			location = "Unknown"
		}
	}

	// If no sensor ID is provided, generate one based on location
	if sensorID == "" {
		switch location {
		case "Living Room":
			sensorID = "1"
		case "Bedroom":
			sensorID = "2"
		case "Kitchen":
			sensorID = "3"
		default:
			sensorID = "0"
		}
	}

	return TemperatureResponse{
		Value:       randomTemperature(),
		Unit:        "°C",
		Timestamp:   time.Now().UTC(),
		Location:    location,
		Status:      "active",
		SensorID:    sensorID,
		SensorType:  "temperature",
		Description: fmt.Sprintf("Temperature in %s", location),
	}
}

// randomTemperature returns a value in the configured range with one decimal place
func randomTemperature() float64 {
	value := minTemperature + rand.Float64()*(maxTemperature-minTemperature)
	return math.Round(value*10) / 10
}

// writeJSON writes the value as a JSON response
func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("Failed to write response: %v", err)
	}
}

// getEnv gets an environment variable or returns a default value
func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}
