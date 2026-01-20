package pubsub

import (
	"context"
	"fmt"
	"time"

	"github.com/nkibbey/poolparty/pkg/queue"
)

func Publisher(ctx context.Context, q queue.Queue) {
	defer fmt.Println("Publisher exiting")
	for i := 1; i <= 15; i++ { // Publish 15 messages
		msg := queue.Message{ID: i, Body: fmt.Sprintf("Task %d", i)}
		err := q.Enqueue(ctx, msg)
		if err != nil {
			fmt.Printf("Publisher error: %v\n", err)
			return // Exit if we cannot enqueue (e.g., shutdown or context cancelled)
		}
		fmt.Printf("Published message %d\n", i)
		time.Sleep(100 * time.Millisecond) // Simulate work
	}
}

func Consumer(id int, q queue.Queue, ctx context.Context) {
	defer fmt.Printf("Consumer %d exiting\n", id)
	for {
		msg, err := q.Dequeue(ctx)
		if err != nil {
			fmt.Printf("Consumer %d error dequeueing: %v\n", id, err)
			return // Exit if the queue is closed or context cancelled
		}
		// Process message (guaranteed to be processed by only one consumer)
		fmt.Printf("Consumer %d processing message ID: %d, body: %s\n", id, msg.ID, msg.Body)
		time.Sleep(200 * time.Millisecond) // Simulate processing time
		// q.doneProcessing()                 // Mark as done after successful processing
	}
}
