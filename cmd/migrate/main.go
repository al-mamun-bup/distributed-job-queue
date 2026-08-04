package main

import (
	"context"
	"fmt"
	"os"

	"hopper/internal/infrastructure/config"
	"hopper/internal/infrastructure/database"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: migrate [up|down]")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "up":
		if err := runMigrateUp(); err != nil {
			fmt.Fprintf(os.Stderr, "migrate up failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("migrations applied")
	case "down":
		if err := runMigrateDown(); err != nil {
			fmt.Fprintf(os.Stderr, "migrate down failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("migrations rolled back")
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", os.Args[1])
		os.Exit(1)
	}
}

func runMigrateUp() error {
	cfg, err := config.Load("config/config.yaml")
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	ctx := context.Background()
	pool, err := database.NewPool(ctx, cfg.Database)
	if err != nil {
		return fmt.Errorf("opening postgres pool: %w", err)
	}
	defer pool.Close()

	if err := database.MigrateUp(ctx, pool); err != nil {
		return fmt.Errorf("running migrate up: %w", err)
	}

	return nil
}

func runMigrateDown() error {
	cfg, err := config.Load("config/config.yaml")
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	ctx := context.Background()
	pool, err := database.NewPool(ctx, cfg.Database)
	if err != nil {
		return fmt.Errorf("opening postgres pool: %w", err)
	}
	defer pool.Close()

	if err := database.MigrateDown(ctx, pool); err != nil {
		return fmt.Errorf("running migrate down: %w", err)
	}

	return nil
}
