package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	telemetryTopic = "device.telemetry"
	maxReadings    = 100
)

// Reading represents a single temperature reading
type Reading struct {
	Metric     string    `json:"metric"`
	Value      float64   `json:"value"`
	Unit       string    `json:"unit"`
	OccurredAt time.Time `json:"occurred_at"`
}

// Store keeps the latest readings of each device in memory
type Store struct {
	mu       sync.RWMutex
	readings map[string][]Reading
}

// NewStore creates an empty Store
func NewStore() *Store {
	return &Store{readings: make(map[string][]Reading)}
}

// Add saves a reading unless it repeats the last reading of the device
func (s *Store) Add(deviceID string, reading Reading) {
	s.mu.Lock()
	defer s.mu.Unlock()

	readings := s.readings[deviceID]
	if len(readings) > 0 && readings[len(readings)-1].OccurredAt.Equal(reading.OccurredAt) {
		return
	}
	readings = append(readings, reading)
	if len(readings) > maxReadings {
		readings = readings[1:]
	}
	s.readings[deviceID] = readings
}

// History returns a copy of the stored readings of a device, oldest first
func (s *Store) History(deviceID string) []Reading {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]Reading(nil), s.readings[deviceID]...)
}

// Consumer saves readings from the device.telemetry topic to the store
type Consumer struct {
	Store *Store
}

// Handle saves a single message. Invalid messages are skipped
func (c *Consumer) Handle(raw []byte) {
	var message struct {
		DeviceID string `json:"device_id"`
		Reading
	}
	if err := json.Unmarshal(raw, &message); err != nil {
		log.Printf("Skipping invalid reading: %v", err)
		return
	}
	if message.DeviceID == "" || message.Metric == "" || message.OccurredAt.IsZero() {
		log.Printf("Skipping incomplete reading: %s", raw)
		return
	}
	c.Store.Add(message.DeviceID, message.Reading)
}

// Run reads the topic until the process exits
func (c *Consumer) Run(ctx context.Context, reader *kafka.Reader) {
	for {
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Printf("Failed to read reading: %v", err)
			time.Sleep(time.Second)
			continue
		}
		c.Handle(message.Value)
	}
}

// API serves the stored readings
type API struct {
	Store *Store
}

// Routes returns the API routes
func (a *API) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /api/v1/telemetry/devices/{id}/latest", a.handleLatest)
	mux.HandleFunc("GET /api/v1/telemetry/devices/{id}/history", a.handleHistory)
	return mux
}

// handleLatest handles GET /api/v1/telemetry/devices/{id}/latest
func (a *API) handleLatest(w http.ResponseWriter, r *http.Request) {
	deviceID := r.PathValue("id")
	readings := a.Store.History(deviceID)
	if len(readings) == 0 {
		writeNotFound(w)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"device_id": deviceID,
		"readings":  readings[len(readings)-1:],
	})
}

// handleHistory handles GET /api/v1/telemetry/devices/{id}/history
func (a *API) handleHistory(w http.ResponseWriter, r *http.Request) {
	deviceID := r.PathValue("id")
	readings := a.Store.History(deviceID)
	if len(readings) == 0 {
		writeNotFound(w)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"device_id": deviceID,
		"readings":  readings,
	})
}

// writeNotFound writes the response for a device without readings
func writeNotFound(w http.ResponseWriter) {
	writeJSON(w, http.StatusNotFound, map[string]string{
		"code":    "DEVICE_NOT_FOUND",
		"message": "Показания устройства не найдены",
	})
}

// writeJSON writes the value as a JSON response
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
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

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{getEnv("KAFKA_BROKER", "localhost:9092")},
		GroupID:     "telemetry-service",
		Topic:       telemetryTopic,
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	store := NewStore()
	consumer := &Consumer{Store: store}
	go consumer.Run(context.Background(), reader)

	addr := getEnv("PORT", ":8083")
	log.Printf("Server starting on %s\n", addr)
	api := &API{Store: store}
	if err := http.ListenAndServe(addr, api.Routes()); err != nil {
		log.Fatalf("Failed to start server: %v\n", err)
	}
}
