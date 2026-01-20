package pubsub

import (
	"context"
	"fmt"
	"time"

	"github.com/nkibbey/poolparty/pkg/queue"
)

func Publisher(ctx context.Context, q queue.Queue, nMsgs int, queueTime time.Duration) {
	defer fmt.Println("Publisher exiting")
	for i := 0; i < nMsgs; i++ { // Publish 15 messages
		msg := queue.Message{ID: i, Body: fmt.Sprintf("Task %d", i)}
		err := q.Enqueue(ctx, msg)
		if err != nil {
			fmt.Printf("Publisher error: %v\n", err)
			return
		}
		fmt.Printf("Published message %d\n", i)
		time.Sleep(queueTime) // Simulate work
	}
}

func Consumer(id int, q queue.Queue, ctx context.Context, processTime time.Duration) {
	defer fmt.Printf("Consumer %d exiting\n", id)
	for {
		msg, err := q.Dequeue(ctx)
		if err != nil {
			fmt.Printf("Consumer %d error dequeueing: %v\n", id, err)
			return // Exit if the queue is closed or context cancelled
		}
		fmt.Printf("Consumer %d processing message ID: %d, body: %s\n", id, msg.ID, msg.Body)
		time.Sleep(processTime) // Simulate processing time
	}
}
