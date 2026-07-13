package etcd

import "time"

// Config holds etcd connection configuration.
type Config struct {
	Endpoints   []string      // e.g. ["localhost:2379"]
	DialTimeout time.Duration // default: 5s
	Username    string
	Password    string

	// Discovery settings
	ServicePrefix            string        // key prefix for service registration, default: "/service/"
	LeaseTTL                 int64         // lease TTL in seconds, default: 10
	RegisterRetryMinInterval time.Duration // initial retry interval after lease loss, default: 1s
	RegisterRetryMaxInterval time.Duration // max retry interval after lease loss, default: 30s
}

func DefaultConfig(endpoints []string) *Config {
	return &Config{
		Endpoints:                endpoints,
		DialTimeout:              5 * time.Second,
		ServicePrefix:            "/service/",
		LeaseTTL:                 10,
		RegisterRetryMinInterval: time.Second,
		RegisterRetryMaxInterval: 30 * time.Second,
	}
}
