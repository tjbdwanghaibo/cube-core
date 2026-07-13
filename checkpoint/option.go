package checkpoint

import "time"

// Option configures the Checkpoint system.
type Option func(*Config)

type SnapshotWALMode string

const (
	SnapshotWALModeAsync   SnapshotWALMode = "async"
	SnapshotWALModeDurable SnapshotWALMode = "durable"
)

// Config holds checkpoint configuration.
type Config struct {
	JournalCap                int           // ring buffer capacity (default: 10000)
	FlushWorkers              int           // concurrent flush goroutines (default: 4)
	BatchSize                 int           // max documents per flush batch (default: 200)
	BatchBytes                int           // max bytes per flush batch (default: 512KB)
	FlushInterval             time.Duration // periodic flush interval (default: 1s)
	RetryBackoff              time.Duration // initial retry backoff (default: 100ms)
	RetryMaxBack              time.Duration // max retry backoff (default: 5s)
	JournalSubmitTimeout      time.Duration // max time Submit waits for journal capacity; 0 blocks
	SnapshotWAL               SnapshotWAL   // optional best-effort snapshot WAL
	SnapshotWALRequired       bool          // reject journal submission if snapshot WAL rejects the batch
	SnapshotWALMode           SnapshotWALMode
	SnapshotWALDurableTimeout time.Duration
}

func defaultConfig() Config {
	return Config{
		JournalCap:                10000,
		FlushWorkers:              4,
		BatchSize:                 200,
		BatchBytes:                512 * 1024,
		FlushInterval:             1 * time.Second,
		RetryBackoff:              100 * time.Millisecond,
		RetryMaxBack:              5 * time.Second,
		SnapshotWALMode:           SnapshotWALModeAsync,
		SnapshotWALDurableTimeout: 20 * time.Millisecond,
	}
}

func WithJournalCap(n int) Option {
	return func(c *Config) { c.JournalCap = n }
}

func WithFlushWorkers(n int) Option {
	return func(c *Config) { c.FlushWorkers = n }
}

func WithBatchSize(n int) Option {
	return func(c *Config) { c.BatchSize = n }
}

func WithBatchBytes(n int) Option {
	return func(c *Config) { c.BatchBytes = n }
}

func WithFlushInterval(d time.Duration) Option {
	return func(c *Config) { c.FlushInterval = d }
}

func WithRetryBackoff(initial, max time.Duration) Option {
	return func(c *Config) {
		c.RetryBackoff = initial
		c.RetryMaxBack = max
	}
}

func WithJournalSubmitTimeout(timeout time.Duration) Option {
	return func(c *Config) {
		if timeout > 0 {
			c.JournalSubmitTimeout = timeout
		}
	}
}

func WithSnapshotWAL(wal SnapshotWAL) Option {
	return func(c *Config) { c.SnapshotWAL = wal }
}

func WithSnapshotWALRequired(required bool) Option {
	return func(c *Config) { c.SnapshotWALRequired = required }
}

func WithSnapshotWALMode(mode SnapshotWALMode) Option {
	return func(c *Config) {
		if mode != "" {
			c.SnapshotWALMode = mode
		}
	}
}

func WithSnapshotWALDurableTimeout(timeout time.Duration) Option {
	return func(c *Config) {
		if timeout > 0 {
			c.SnapshotWALDurableTimeout = timeout
		}
	}
}
