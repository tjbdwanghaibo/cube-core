package nats

import "time"

// Config holds NATS connection configuration.
type Config struct {
	URL           string
	ReconnectWait time.Duration // default: 1s
	MaxReconnects int           // default: -1 (unlimited)
	PingInterval  time.Duration // default: 20s
	DrainTimeout  time.Duration // default: 10s

	OnDisconnect func(error)
	OnReconnect  func()
}

func DefaultConfig(url string) *Config {
	return &Config{
		URL:           url,
		ReconnectWait: time.Second,
		MaxReconnects: -1,
		PingInterval:  20 * time.Second,
		DrainTimeout:  10 * time.Second,
	}
}
