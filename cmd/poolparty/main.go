package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/nkibbey/poolparty/internal/pubsub"
	"github.com/nkibbey/poolparty/internal/pubsub/bq"
)

var (
	PrintVersion = flag.Bool("v", false, "Display build info and exit")

	Version   = "devel"
	GitCommit = "devel"
	BuildTime = "devel"
)

// buildInfo provides information about this build
func buildInfo() string {
	return fmt.Sprintf("Version: %s\nBuild Time: %s\nGitCommit: %s", Version, BuildTime, GitCommit)
}

func main() {
	flag.Parse()
	if *PrintVersion {
		log.Printf("------BUILD INFO-----\n%s\n-----------------------------------------", buildInfo())
		return
	}
	const QueueCapacity = 5
	const NumConsumers = 3

	// Root context for the whole application life cycle
	appCtx, appCancel := context.WithCancel(context.Background())
	queue := bq.NewBQ(QueueCapacity)

	// Start consumers
	for i := 1; i <= NumConsumers; i++ {
		go pubsub.Consumer(i, queue, appCtx)
	}

	// Start publisher
	go pubsub.Publisher(appCtx, queue)

	// Wait for a bit and then initiate graceful shutdown
	time.Sleep(2 * time.Second)
	fmt.Println("\nInitiating graceful shutdown...")

	// Create a context with a timeout for the shutdown process itself
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := queue.Shutdown(shutdownCtx); err != nil {
		fmt.Printf("Shutdown error: %v\n", err)
	}
	fmt.Println("Shutdown complete. Waiting for all goroutines to finish...")

	// Cancel the application context to signal all consumers to stop their Dequeue calls
	appCancel()

	// In a real application, you might use an additional WaitGroup to ensure consumers have exited,
	// but here the main goroutine will exit after the graceful shutdown logic has completed
	// and the appCtx is cancelled.
}
