package bq

import (
	"context"
	"fmt"
	"sync"

	"github.com/nkibbey/poolparty/pkg/queue"
)

const (
	CLOSED_BQ_ERR = "queue channel closed, no more messages"
)

// BQ is a Bounded Queue and implements the Queue interface
type BQ struct {
	queueChan    chan queue.Message // Main channel for messages
	shutdownChan chan struct{}      // Channel to signal shutdown is complete
	wg           sync.WaitGroup     // WaitGroup to track active consumers/producers
	mu           sync.Mutex         // Mutex to protect internal state
	isShutdown   bool               // Flag to indicate if the queue is shutting down
}

// NewBQ creates a new bounded queue with a specific capacity
func NewBQ(capacity int) *BQ {
	q := &BQ{
		queueChan:    make(chan queue.Message, capacity),
		shutdownChan: make(chan struct{}),
	}
	return q
}

// Enqueue adds a message to the queue. It blocks if the queue is full or during shutdown.
func (q *BQ) Enqueue(ctx context.Context, msg queue.Message) error {
	q.mu.Lock()
	if q.isShutdown {
		q.mu.Unlock()
		return fmt.Errorf("queue is shut down, cannot enqueue message ID %d", msg.ID)
	}
	q.wg.Add(1) // Increment WaitGroup before starting work, inside the lock to prevent race on shutdown check
	q.mu.Unlock()

	select {
	case <-ctx.Done():
		q.wg.Done() // Decrement if context cancelled before enqueuing
		return ctx.Err()
	case q.queueChan <- msg:
		return nil
	case <-q.shutdownChan:
		q.wg.Done() // Decrement if shutdown occurred during the select
		return fmt.Errorf("queue shut down during enqueue of message ID %d", msg.ID)
	}
}

// Dequeue retrieves a message from the queue. It blocks if the queue is empty.
func (q *BQ) Dequeue(ctx context.Context) (queue.Message, error) {
	select {
	case <-ctx.Done():
		return queue.Message{}, ctx.Err()
	case msg, ok := <-q.queueChan:
		if !ok {
			return queue.Message{}, fmt.Errorf(CLOSED_BQ_ERR)
		}
		go func() {
			q.wg.Done()
		}()
		return msg, nil
	}
}

// Shutdown initiates a graceful shutdown, stops accepting new messages, and waits for existing ones to drain.
func (q *BQ) Shutdown(ctx context.Context) error {
	q.mu.Lock()
	if q.isShutdown {
		q.mu.Unlock()
		return fmt.Errorf("queue already shut down")
	}
	q.isShutdown = true
	close(q.shutdownChan) // Signal all blocked Enqueue operations to unblock
	q.mu.Unlock()

	// Wait for all in-flight or pending enqueue operations to complete
	waitChan := make(chan struct{})
	go func() {
		q.wg.Wait()
		close(waitChan)
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("shutdown context cancelled: %w", ctx.Err())
	case <-waitChan:
		// All pending Enqueue calls have completed (either succeeded or returned an error).
		// Now close the message channel to signal consumers that no more messages will arrive.
		close(q.queueChan)
		return nil
	}
}
