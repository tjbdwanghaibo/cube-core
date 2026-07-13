package redis

import "time"

// Config holds Redis connection configuration.
type Config struct {
	Addr         string // "host:port", default: "localhost:6379"
	Password     string
	DB           int           // default: 0
	PoolSize     int           // default: 10
	MinIdleConns int           // default: 5
	DialTimeout  time.Duration // default: 5s
	ReadTimeout  time.Duration // default: 3s
	WriteTimeout time.Duration // default: 3s
	MaxRetries   int           // default: 3

	// Cluster mode (if non-empty, Addr is ignored)
	ClusterAddrs []string
}

func DefaultConfig(addr string) *Config {
	return &Config{
		Addr:         addr,
		DB:           0,
		PoolSize:     10,
		MinIdleConns: 5,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		MaxRetries:   3,
	}
}

// IsCluster returns true if cluster mode is configured.
func (c *Config) IsCluster() bool {
	return len(c.ClusterAddrs) > 0
}
