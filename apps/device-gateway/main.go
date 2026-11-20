package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	commandsTopic  = "device.commands"
	resultsTopic   = "device.command-results"
	telemetryTopic = "device.telemetry"
	pollInterval   = 5 * time.Second
)

// Sensor represents the fields of a monolith sensor used by the gateway
type Sensor struct {
	ID          int       `json:"id"`
	Type        string    `json:"type"`
	Value       float64   `json:"value"`
	Unit        string    `json:"unit"`
	LastUpdated time.Time `json:"last_updated"`
}

// DeviceCommand represents a message from the device.commands topic
type DeviceCommand struct {
	CommandID string    `json:"command_id"`
	DeviceID  string    `json:"device_id"`
	Action    string    `json:"action"`
	ExpiresAt time.Time `json:"expires_at"`
}

// CommandResult represents a message for the device.command-results topic
type CommandResult struct {
	CommandID  string    `json:"command_id"`
	DeviceID   string    `json:"device_id"`
	Status     string    `json:"status"`
	OccurredAt time.Time `json:"occurred_at"`
}

// TelemetryReading represents a message for the device.telemetry topic
type TelemetryReading struct {
	DeviceID   string    `json:"device_id"`
	Metric     string    `json:"metric"`
	Value      float64   `json:"value"`
	Unit       string    `json:"unit"`
	OccurredAt time.Time `json:"occurred_at"`
}

// Monolith is the part of the monolith API used by the gateway
type Monolith interface {
	ListSensors() ([]Sensor, error)
	SetStatus(deviceID, status string) error
}

// Publisher publishes JSON messages with the device ID as the key
type Publisher interface {
	Publish(ctx context.Context, topic, deviceID string, value any) error
}

// MonolithClient calls the monolith REST API
type MonolithClient struct {
	BaseURL    string
	HTTPClient *http.Client
}

// ListSensors fetches all sensors from the monolith
func (m *MonolithClient) ListSensors() ([]Sensor, error) {
	resp, err := m.HTTPClient.Get(m.BaseURL + "/api/v1/sensors")
	if err != nil {
		return nil, fmt.Errorf("error fetching sensors: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var sensors []Sensor
	if err := json.NewDecoder(resp.Body).Decode(&sensors); err != nil {
		return nil, fmt.Errorf("error decoding sensors: %w", err)
	}
	return sensors, nil
}

// SetStatus changes the status of a sensor in the monolith
func (m *MonolithClient) SetStatus(deviceID, status string) error {
	body, err := json.Marshal(map[string]string{"status": status})
	if err != nil {
		return err
	}

	sensorURL := m.BaseURL + "/api/v1/sensors/" + url.PathEscape(deviceID)
	req, err := http.NewRequest(http.MethodPut, sensorURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := m.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("error updating sensor: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	return nil
}

// KafkaPublisher publishes messages to Kafka
type KafkaPublisher struct {
	Writer *kafka.Writer
}

// Publish sends a JSON message to a topic
func (p *KafkaPublisher) Publish(ctx context.Context, topic, deviceID string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return p.Writer.WriteMessages(ctx, kafka.Message{Topic: topic, Key: []byte(deviceID), Value: data})
}

// Gateway connects devices to the event broker. In the MVP the monolith plays the role of the devices.
type Gateway struct {
	Monolith  Monolith
	Publisher Publisher
}

// PublishTelemetry publishes the current reading of every temperature sensor
func (g *Gateway) PublishTelemetry(ctx context.Context) error {
	sensors, err := g.Monolith.ListSensors()
	if err != nil {
		return err
	}

	for _, sensor := range sensors {
		if sensor.Type != "temperature" {
			continue
		}
		unit := sensor.Unit
		if unit == "" {
			unit = "°C"
		}
		deviceID := strconv.Itoa(sensor.ID)
		err := g.Publisher.Publish(ctx, telemetryTopic, deviceID, TelemetryReading{
			DeviceID:   deviceID,
			Metric:     "temperature",
			Value:      sensor.Value,
			Unit:       unit,
			OccurredAt: sensor.LastUpdated,
		})
		if err != nil {
			return fmt.Errorf("error publishing reading: %w", err)
		}
	}
	return nil
}

// HandleCommand executes a command and publishes its result
func (g *Gateway) HandleCommand(ctx context.Context, raw []byte) error {
	var command DeviceCommand
	if err := json.Unmarshal(raw, &command); err != nil {
		return fmt.Errorf("error decoding command: %w", err)
	}

	result := CommandResult{
		CommandID:  command.CommandID,
		DeviceID:   command.DeviceID,
		Status:     g.execute(command),
		OccurredAt: time.Now().UTC(),
	}
	return g.Publisher.Publish(ctx, resultsTopic, command.DeviceID, result)
}

// execute applies a command and returns its final status
func (g *Gateway) execute(command DeviceCommand) string {
	if time.Now().After(command.ExpiresAt) {
		return "EXPIRED"
	}

	var status string
	switch command.Action {
	case "TURN_ON":
		status = "active"
	case "TURN_OFF":
		status = "inactive"
	default:
		log.Printf("Unknown action %q in command %s", command.Action, command.CommandID)
		return "FAILED"
	}

	if err := g.Monolith.SetStatus(command.DeviceID, status); err != nil {
		log.Printf("Failed to execute command %s: %v", command.CommandID, err)
		return "FAILED"
	}
	return "SUCCEEDED"
}

// runTelemetry publishes telemetry until the process exits
func (g *Gateway) runTelemetry(ctx context.Context) {
	for {
		if err := g.PublishTelemetry(ctx); err != nil {
			log.Printf("Failed to publish telemetry: %v", err)
		}
		time.Sleep(pollInterval)
	}
}

// runCommands handles commands until the process exits
func (g *Gateway) runCommands(ctx context.Context, reader *kafka.Reader) {
	for {
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Printf("Failed to read command: %v", err)
			time.Sleep(time.Second)
			continue
		}
		if err := g.HandleCommand(ctx, message.Value); err != nil {
			log.Printf("Failed to handle command: %v", err)
		}
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
	broker := getEnv("KAFKA_BROKER", "localhost:9092")
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		GroupID:     "device-gateway",
		Topic:       commandsTopic,
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	gateway := &Gateway{
		Monolith: &MonolithClient{
			BaseURL:    getEnv("MONOLITH_URL", "http://localhost:8080"),
			HTTPClient: &http.Client{Timeout: 30 * time.Second},
		},
		Publisher: &KafkaPublisher{Writer: &kafka.Writer{Addr: kafka.TCP(broker), Balancer: &kafka.Hash{}}},
	}

	ctx := context.Background()
	go gateway.runTelemetry(ctx)
	log.Println("Device gateway started")
	gateway.runCommands(ctx, reader)
}
