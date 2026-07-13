package mongo

import "time"

// Config holds MongoDB connection configuration.
type Config struct {
	URI            string        // "mongodb://localhost:27017"
	ConnectTimeout time.Duration // default: 10s
	MaxPoolSize    uint64        // default: 100
	MinPoolSize    uint64        // default: 10
	MaxIdleTime    time.Duration // default: 5m
}

func DefaultConfig(uri string) *Config {
	return &Config{
		URI:            uri,
		ConnectTimeout: 10 * time.Second,
		MaxPoolSize:    100,
		MinPoolSize:    10,
		MaxIdleTime:    5 * time.Minute,
	}
}
