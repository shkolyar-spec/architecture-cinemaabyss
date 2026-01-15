package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

type HealthResponse struct {
	Status bool `json:"status"`
}

type SuccessResponse struct {
	Status  string `json:"status"`
	EventID string `json:"event_id"`
}

func env(key, def string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	return v
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func main() {
	port := env("PORT", "8082")
	brokers := strings.Split(env("KAFKA_BROKERS", "kafka:9092"), ",")

	// Writers (producers) per topic
	movieWriter := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        "movie-events",
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
	}
	userWriter := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        "user-events",
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
	}
	paymentWriter := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        "payment-events",
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
	}

	// Readers (consumers)
	ctx := context.Background()
	go consumeTopic(ctx, brokers, "movie-events", "events-service-movie-consumer")
	go consumeTopic(ctx, brokers, "user-events", "events-service-user-consumer")
	go consumeTopic(ctx, brokers, "payment-events", "events-service-payment-consumer")

	mux := http.NewServeMux()

	mux.HandleFunc("/api/events/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, HealthResponse{Status: true})
	})

	mux.HandleFunc("/api/events/movie", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		eventID := uuid.NewString()
		payload["event_id"] = eventID
		payload["event_type"] = "movie"
		payload["occurred_at"] = time.Now().UTC().Format(time.RFC3339)

		b, _ := json.Marshal(payload)
		if err := movieWriter.WriteMessages(r.Context(), kafka.Message{
			Key:   []byte(eventID),
			Value: b,
		}); err != nil {
			log.Printf("kafka write movie-events failed: %v", err)
			http.Error(w, "kafka write failed", http.StatusBadGateway)
			return
		}
		writeJSON(w, http.StatusCreated, SuccessResponse{Status: "success", EventID: eventID})
	})

	mux.HandleFunc("/api/events/user", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		eventID := uuid.NewString()
		payload["event_id"] = eventID
		payload["event_type"] = "user"
		payload["occurred_at"] = time.Now().UTC().Format(time.RFC3339)

		b, _ := json.Marshal(payload)
		if err := userWriter.WriteMessages(r.Context(), kafka.Message{
			Key:   []byte(eventID),
			Value: b,
		}); err != nil {
			log.Printf("kafka write user-events failed: %v", err)
			http.Error(w, "kafka write failed", http.StatusBadGateway)
			return
		}
		writeJSON(w, http.StatusCreated, SuccessResponse{Status: "success", EventID: eventID})
	})

	mux.HandleFunc("/api/events/payment", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		eventID := uuid.NewString()
		payload["event_id"] = eventID
		payload["event_type"] = "payment"
		payload["occurred_at"] = time.Now().UTC().Format(time.RFC3339)

		b, _ := json.Marshal(payload)
		if err := paymentWriter.WriteMessages(r.Context(), kafka.Message{
			Key:   []byte(eventID),
			Value: b,
		}); err != nil {
			log.Printf("kafka write payment-events failed: %v", err)
			http.Error(w, "kafka write failed", http.StatusBadGateway)
			return
		}
		writeJSON(w, http.StatusCreated, SuccessResponse{Status: "success", EventID: eventID})
	})

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("events-service listening on :%s (brokers=%v)", port, brokers)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("listen: %v", err)
	}
}

func consumeTopic(ctx context.Context, brokers []string, topic, groupID string) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer r.Close()

	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			log.Printf("consumer(%s) stopped: %v", topic, err)
			return
		}
		log.Printf("consumed topic=%s partition=%d offset=%d key=%s value=%s", topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	}
}
