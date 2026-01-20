package queue

import "context"

type Message struct {
	ID   int
	Body string
}

type Queue interface {
	Enqueue(ctx context.Context, msg Message) error
	Dequeue(ctx context.Context) (Message, error)
	Shutdown(ctx context.Context) error
}
