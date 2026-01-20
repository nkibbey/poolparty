package bq_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nkibbey/poolparty/internal/pubsub/bq"
	"github.com/nkibbey/poolparty/pkg/queue"
)

func consumerTestHelper(id int, q *bq.BQ, wg *sync.WaitGroup, results chan<- queue.Message) {
	defer wg.Done()
	for {
		msg, err := q.Dequeue(context.Background())
		if err != nil {
			// Expected error when the queue channel is closed gracefully
			if err.Error() == bq.CLOSED_BQ_ERR {
				return
			}
			// Other errors should ideally not happen in this test setup
			fmt.Printf("Consumer %d error dequeueing: %v\n", id, err)
			return
		}

		// Simulate processing and record the result
		time.Sleep(1 * time.Millisecond) // Minimal processing time
		results <- msg
		// q.doneProcessing() // Mark work as done
	}
}

func TestCompetingConsumersCoreConstraints(t *testing.T) {
	const (
		QueueCapacity = 5
		NumConsumers  = 3
		NumMessages   = 20
		Timeout       = 5 * time.Second
	)

	// Setup
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)
	defer cancel()

	bq := bq.NewBQ(QueueCapacity)
	resultsChan := make(chan queue.Message, NumMessages)
	consumerWG := sync.WaitGroup{}

	// Start Consumers
	for i := 1; i <= NumConsumers; i++ {
		consumerWG.Add(1)
		go consumerTestHelper(i, bq, &consumerWG, resultsChan)
	}

	// Start Publisher
	publisherWG := sync.WaitGroup{}
	publisherWG.Add(1)
	go func() {
		defer publisherWG.Done()
		for i := 1; i <= NumMessages; i++ {
			msg := queue.Message{ID: i, Body: fmt.Sprintf("Task %d", i)}
			if err := bq.Enqueue(ctx, msg); err != nil {
				// We might hit a context timeout if the test is too slow, or shutdown error
				t.Logf("Publisher stopped enqueueing: %v\n", err)
				return
			}
		}
	}()

	// Wait for publishing to complete, or timeout
	publisherWG.Wait()

	// Initiate graceful shutdown
	if err := bq.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown failed unexpectedly: %v", err)
	}

	// Close the results channel after all consumers exit their loops (which happens after queue closure)
	go func() {
		consumerWG.Wait()
		close(resultsChan)
	}()

	// Collect results
	processedMessages := make(map[int]int)
	for msg := range resultsChan {
		processedMessages[msg.ID]++
	}

	// --- Assertions ---

	// 1. All messages must be processed
	if len(processedMessages) != NumMessages {
		t.Errorf("Expected %d messages processed, but got %d", NumMessages, len(processedMessages))
	}

	// 2. Each message must be processed exactly once (Competing Consumers constraint)
	for id, count := range processedMessages {
		if count != 1 {
			t.Errorf("Message ID %d processed %d times; expected exactly once", id, count)
		}
	}

	// 3. Test blocking behavior (implicitly tested by using a bounded queue and multiple producers/consumers)
	// The system shouldn't deadlock or busy wait. The successful completion of the test
	// with Shutdown passing implies the blocking behavior worked correctly.

	// 4. Test graceful shutdown (implicitly tested by calling bq.Shutdown() and successfully receiving all messages)
}

func TestEnqueueWhenShutdown(t *testing.T) {
	b := bq.NewBQ(1)
	ctx := context.Background()

	// Immediately shut down the queue
	if err := b.Shutdown(ctx); err != nil {
		t.Fatal("Shutdown failed")
	}

	msg := queue.Message{ID: 99, Body: "Should fail"}
	err := b.Enqueue(ctx, msg)

	if err == nil {
		t.Error("Expected an error when enqueueing into a shut down queue, but got nil")
	}

	expectedError := "queue is shut down, cannot enqueue"
	if err != nil && !strings.Contains(err.Error(), expectedError) {
		t.Errorf("Expected error message containing '%s', got: %s", expectedError, err.Error())
	}
}
