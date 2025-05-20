package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/segmentio/kafka-go"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type MovieEvent struct {
	MovieId     int      `json:"movie_id" binding:"required"`
	Title       string   `json:"title" binding:"required"`
	Action      string   `json:"action" binding:"required"`
	UserId      int      `json:"user_id,omitempty"`
	Rating      float64  `json:"rating,omitempty"`
	Genres      []string `json:"genres,omitempty"`
	Description string   `json:"description,omitempty"`
}

type UserEvent struct {
	UserId    int       `json:"user_id" binding:"required"`
	Username  string    `json:"username,omitempty"`
	Email     string    `json:"email,omitempty"`
	Action    string    `json:"action" binding:"required"`
	Timestamp time.Time `json:"timestamp" binding:"required"`
}

type PaymentEvent struct {
	PaymentId  int       `json:"payment_id" binding:"required"`
	UserId     int       `json:"user_id" binding:"required"`
	Amount     float64   `json:"amount" binding:"required"`
	Status     string    `json:"status" binding:"required"`
	Timestamp  time.Time `json:"timestamp" binding:"required"`
	MethodType string    `json:"method_type,omitempty"`
}

var kafkaBrokersAddress = getEnv("KAFKA_BROKERS", "")

var topics = map[string]string{
	"movie-events":   "movie-events",
	"user-events":    "user-events",
	"payment-events": "payment-events",
}

var producers = map[string]*kafka.Writer{}

func main() {

	for _, topic := range topics {
		producers[topic] = createProducer(topic)
		go consumeTopic(topic)
	}

	router := gin.Default()
	apiRoutes := router.Group("/api/events")
	// Health check endpoint
	apiRoutes.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status": true,
		})
	})
	apiRoutes.POST("/movie", createMovieEvent)
	apiRoutes.POST("/user", createUserEvent)
	apiRoutes.POST("/payment", createPaymentEvent)

	// Start server
	srv := &http.Server{
		Addr:    ":" + getEnv("PORT", ":8000"),
		Handler: router,
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

func createMovieEvent(c *gin.Context) {
	var movieEvent MovieEvent

	if err := c.ShouldBindJSON(&movieEvent); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	movieEventToSend, err := json.Marshal(movieEvent)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"internal error": err.Error()})
		return
	}

	err = produceTopic(topics["movie-events"], movieEventToSend)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"internal error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"status":  "success",
		"message": "Сообщение успешно отправлено",
	})
}

func createUserEvent(c *gin.Context) {
	var userEvent UserEvent

	if err := c.ShouldBindJSON(&userEvent); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	userEventToSend, err := json.Marshal(userEvent)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"internal error": err.Error()})
		return
	}

	err = produceTopic(topics["user-events"], userEventToSend)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"internal error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"status":  "success",
		"message": "Сообщение успешно отправлено",
	})
}

func createPaymentEvent(c *gin.Context) {
	var paymentEvent PaymentEvent

	if err := c.ShouldBindJSON(&paymentEvent); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	paymentEventToSend, err := json.Marshal(paymentEvent)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"internal error": err.Error()})
		return
	}

	err = produceTopic(topics["payment-events"], paymentEventToSend)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"internal error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"status":  "success",
		"message": "Сообщение успешно отправлено",
	})
}

func createProducer(topic string) *kafka.Writer {
	w := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBrokersAddress),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}

	return w
}

func produceTopic(topic string, message []byte) error {

	w := *producers[topic]

	err := w.WriteMessages(context.Background(),
		kafka.Message{
			Value: message,
		},
	)
	if err != nil {
		log.Print("failed to write messages:", err)
	}

	if err := w.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}

	return err
}

func consumeTopic(topic string) {

	ctx := context.Background()

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaBrokersAddress},
		GroupID:  "events-service-group",
		Topic:    topic,
		MaxBytes: 10e6, // 10MB
	})

	defer r.Close()

	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			log.Printf("Error in topic %s: %v\n", topic, err)
			continue
		}
		fmt.Printf("message at topic: %v; partition: %v; offset: %v; value: %s", m.Topic, m.Partition, m.Offset, string(m.Value))
	}

	//if err := r.Close(); err != nil {
	//	log.Fatal("failed to close reader:", err)
	//}
}

// getEnv gets an environment variable or returns a default value
func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}
