package postgres

import (
	"context"
	"fmt"
	"regexp"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var listenChannelNamePattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

type QueueListener struct {
	pool       *pgxpool.Pool
	channel    string
	notifyCh   chan struct{}
	backendPID atomic.Int32
}

func NewQueueListener(pool *pgxpool.Pool, channel string) (*QueueListener, error) {
	if !listenChannelNamePattern.MatchString(channel) {
		return nil, fmt.Errorf("creating queue listener: invalid channel %q", channel)
	}

	return &QueueListener{
		pool:     pool,
		channel:  channel,
		notifyCh: make(chan struct{}, 1),
	}, nil
}

func (l *QueueListener) Notifications() <-chan struct{} {
	return l.notifyCh
}

func (l *QueueListener) BackendPID() int32 {
	return l.backendPID.Load()
}

func (l *QueueListener) Run(ctx context.Context) error {
	backoff := 100 * time.Millisecond
	maxBackoff := 5 * time.Second

	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		conn, err := l.pool.Acquire(ctx)
		if err != nil {
			if waitErr := sleepWithContext(ctx, backoff); waitErr != nil {
				return nil
			}
			backoff = nextBackoff(backoff, maxBackoff)
			continue
		}

		if _, err := conn.Exec(ctx, fmt.Sprintf("LISTEN %s", l.channel)); err != nil {
			conn.Release()
			if waitErr := sleepWithContext(ctx, backoff); waitErr != nil {
				return nil
			}
			backoff = nextBackoff(backoff, maxBackoff)
			continue
		}

		l.backendPID.Store(int32(conn.Conn().PgConn().PID()))
		backoff = 100 * time.Millisecond

		waitErr := l.waitNotifications(ctx, conn)
		l.backendPID.Store(0)
		conn.Release()
		if waitErr != nil {
			if err := ctx.Err(); err != nil {
				return nil
			}
			if err := sleepWithContext(ctx, backoff); err != nil {
				return nil
			}
			backoff = nextBackoff(backoff, maxBackoff)
			continue
		}
	}
}

func (l *QueueListener) waitNotifications(ctx context.Context, conn *pgxpool.Conn) error {
	for {
		notification, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			return fmt.Errorf("waiting for notification: %w", err)
		}
		if notification.Channel != l.channel {
			continue
		}
		select {
		case l.notifyCh <- struct{}{}:
		default:
		}
	}
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func nextBackoff(current, ceiling time.Duration) time.Duration {
	next := current * 2
	if next > ceiling {
		return ceiling
	}
	return next
}
