package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
)

func main() {
	ctx := context.Background()
	if err := run(ctx, os.Args, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, w io.Writer) error {
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	// do app stuff...

	return nil
}
