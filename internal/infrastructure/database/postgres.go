// Package database manages Postgres resources.
package database

import (
	"context"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"hopper/internal/infrastructure/config"
	"hopper/migrations"
)

func NewPool(ctx context.Context, cfg config.DatabaseConfig) (*pgxpool.Pool, error) {
	dsn := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.Host,
		cfg.Port,
		cfg.User,
		cfg.Password,
		cfg.Name,
		cfg.SSLMode,
	)

	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parsing postgres config: %w", err)
	}
	poolConfig.MaxConns = cfg.MaxConns
	poolConfig.MinConns = cfg.MinConns
	poolConfig.MaxConnLifetime = cfg.MaxConnLifetime

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("creating postgres pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pinging postgres: %w", err)
	}

	return pool, nil
}

func MigrateUp(ctx context.Context, pool *pgxpool.Pool) error {
	files, err := listMigrationFiles(".up.sql")
	if err != nil {
		return fmt.Errorf("listing up migrations: %w", err)
	}

	if err := ensureMigrationTable(ctx, pool); err != nil {
		return fmt.Errorf("ensuring migration table: %w", err)
	}

	for _, file := range files {
		applied, err := isMigrationApplied(ctx, pool, file)
		if err != nil {
			return fmt.Errorf("checking migration %s: %w", file, err)
		}
		if applied {
			continue
		}

		sqlBody, err := readMigration(file)
		if err != nil {
			return fmt.Errorf("reading migration %s: %w", file, err)
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("beginning migration tx %s: %w", file, err)
		}

		if _, err := tx.Exec(ctx, sqlBody); err != nil {
			if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
				return fmt.Errorf("rolling back failed migration %s: %w", file, rollbackErr)
			}
			return fmt.Errorf("executing migration %s: %w", file, err)
		}

		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, file); err != nil {
			if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
				return fmt.Errorf("rolling back failed migration insert %s: %w", file, rollbackErr)
			}
			return fmt.Errorf("recording migration %s: %w", file, err)
		}

		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("committing migration %s: %w", file, err)
		}
	}

	return nil
}

func MigrateDown(ctx context.Context, pool *pgxpool.Pool) error {
	files, err := listMigrationFiles(".down.sql")
	if err != nil {
		return fmt.Errorf("listing down migrations: %w", err)
	}

	sort.Sort(sort.Reverse(sort.StringSlice(files)))

	for _, file := range files {
		sqlBody, err := readMigration(file)
		if err != nil {
			return fmt.Errorf("reading migration %s: %w", file, err)
		}
		if _, err := pool.Exec(ctx, sqlBody); err != nil {
			return fmt.Errorf("executing down migration %s: %w", file, err)
		}
	}

	if _, err := pool.Exec(ctx, `DROP TABLE IF EXISTS schema_migrations`); err != nil {
		return fmt.Errorf("dropping schema_migrations: %w", err)
	}

	return nil
}

func listMigrationFiles(suffix string) ([]string, error) {
	dirEntries, err := fs.ReadDir(migrations.Files, ".")
	if err != nil {
		return nil, fmt.Errorf("reading migrations dir: %w", err)
	}

	out := make([]string, 0, len(dirEntries))
	for _, entry := range dirEntries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, suffix) {
			continue
		}
		out = append(out, name)
	}
	sort.Strings(out)

	return out, nil
}

func readMigration(name string) (string, error) {
	body, err := migrations.Files.ReadFile(name)
	if err != nil {
		return "", fmt.Errorf("reading embedded file: %w", err)
	}
	return string(body), nil
}

func ensureMigrationTable(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version text PRIMARY KEY,
			applied_at timestamptz NOT NULL DEFAULT now()
		)
	`)
	if err != nil {
		return fmt.Errorf("creating schema_migrations table: %w", err)
	}

	return nil
}

func isMigrationApplied(ctx context.Context, pool *pgxpool.Pool, version string) (bool, error) {
	const query = `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`
	var exists bool
	if err := pool.QueryRow(ctx, query, version).Scan(&exists); err != nil {
		return false, fmt.Errorf("querying schema_migrations: %w", err)
	}
	return exists, nil
}
