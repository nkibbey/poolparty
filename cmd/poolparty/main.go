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

const (
	QueueCapacity = 5
	NumConsumers  = 10
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

	// Root context for the whole application life cycle
	appCtx, appCancel := context.WithCancel(context.Background())
	queue := bq.NewBQ(QueueCapacity)

	for i := 1; i <= NumConsumers; i++ {
		go pubsub.Consumer(i, queue, appCtx, 1*time.Second)
	}
	go pubsub.Publisher(appCtx, queue, 30, 10*time.Millisecond)

	// Wait for a bit and then initiate graceful shutdown
	time.Sleep(5 * time.Second)
	fmt.Println("\nInitiating graceful shutdown...")

	// Create a context with a timeout for the shutdown process itself
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := queue.Shutdown(shutdownCtx); err != nil {
		fmt.Printf("Shutdown error: %v\n", err)
	}

	fmt.Println("Shutdown complete. Cancelling ctx...")
	appCancel()
}
