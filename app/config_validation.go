package app

import (
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

func ValidateServiceConfig(cfg *viper.Viper) error {
	if cfg == nil {
		return errors.New("config: viper is nil")
	}
	var errs []error
	serverType := strings.TrimSpace(cfg.GetString("server_type"))
	if serverType == "" {
		errs = append(errs, errors.New("config: server_type is required"))
	}
	if cfg.GetInt32("sid") <= 0 {
		errs = append(errs, errors.New("config: sid must be positive"))
	}
	if cfg.GetBool("ops.admin_enabled") {
		token := strings.TrimSpace(cfg.GetString("ops.admin_token"))
		if token == "" {
			errs = append(errs, errors.New("config: ops.admin_token is required when ops.admin_enabled=true"))
		}
		if strings.HasPrefix(token, "dev-") && !cfg.GetBool("ops.allow_dev_token") {
			errs = append(errs, errors.New("config: dev ops.admin_token requires ops.allow_dev_token=true"))
		}
	}
	validateTransport(&errs, cfg, "nats.rpc.transport", "core", "nats", "jetstream", "js")
	if isJetStreamTransport(cfg.GetString("nats.rpc.transport")) {
		validateDurationIfSet(&errs, cfg, "nats.rpc.ack_wait")
		validateDurationIfSet(&errs, cfg, "nats.rpc.request_ttl")
		validateDurationIfSet(&errs, cfg, "nats.rpc.call_timeout")
		validateDurationIfSet(&errs, cfg, "nats.rpc.stream_max_age")
		validateDurationIfSet(&errs, cfg, "nats.rpc.duplicates")
		validateDurationIfSet(&errs, cfg, "nats.rpc.setup_timeout")
		validatePositiveIntIfSet(&errs, cfg, "nats.rpc.max_deliver")
		validateNonNegativeIntIfSet(&errs, cfg, "nats.rpc.replicas")
		validateNonNegativeInt64IfSet(&errs, cfg, "nats.rpc.max_bytes")
	}
	if cfg.GetBool("nats.reliable.enabled") {
		validateDurationIfSet(&errs, cfg, "nats.reliable.inbox_ttl")
		validateDurationIfSet(&errs, cfg, "nats.reliable.dlq_ttl")
	}
	validateTransport(&errs, cfg, "sync.transport", "nats", "jetstream", "js")
	if isJetStreamTransport(cfg.GetString("sync.transport")) {
		validateDurationIfSet(&errs, cfg, "sync.ack_wait")
		validateDurationIfSet(&errs, cfg, "sync.stream_max_age")
		validateDurationIfSet(&errs, cfg, "sync.duplicates")
		validateDurationIfSet(&errs, cfg, "sync.setup_timeout")
		validateDurationIfSet(&errs, cfg, "sync.publish_timeout")
		validatePositiveIntIfSet(&errs, cfg, "sync.max_deliver")
	}
	validateNonNegativeIntIfSet(&errs, cfg, "remote_entity.sync_retry_queue_cap")
	validatePositiveDurationIfSet(&errs, cfg, "remote_entity.lock_ttl")
	validatePositiveDurationIfSet(&errs, cfg, "remote_entity.op_timeout")
	validateProductionServiceConfig(&errs, cfg, serverType)
	return errors.Join(errs...)
}

func validateProductionServiceConfig(errs *[]error, cfg *viper.Viper, serverType string) {
	if !isProductionServiceConfig(cfg) {
		return
	}
	switch strings.ToLower(strings.TrimSpace(serverType)) {
	case "game":
		if !cfg.GetBool("player.login_auth_required") {
			*errs = append(*errs, errors.New("config: production game requires player.login_auth_required=true"))
		}
		validateProductionSecret(errs, cfg, "player.login_secret")
		if strings.TrimSpace(cfg.GetString("redis.addr")) == "" {
			*errs = append(*errs, errors.New("config: production game requires redis.addr"))
		}
		if !cfg.GetBool("player_protocol.rate_limit.enabled") {
			*errs = append(*errs, errors.New("config: production game requires player_protocol.rate_limit.enabled=true"))
		}
		if !cfg.GetBool("save_load.wal.enabled") {
			*errs = append(*errs, errors.New("config: production game requires save_load.wal.enabled=true"))
		}
		if !cfg.GetBool("save_load.wal.required") {
			*errs = append(*errs, errors.New("config: production game requires save_load.wal.required=true"))
		}
		if strings.ToLower(strings.TrimSpace(cfg.GetString("save_load.wal.mode"))) != "durable" {
			*errs = append(*errs, errors.New("config: production game requires save_load.wal.mode=durable"))
		}
		if strings.ToLower(strings.TrimSpace(cfg.GetString("instance.client_mode"))) == "local" {
			*errs = append(*errs, errors.New("config: production game must not use instance.client_mode=local"))
		}
	case "instance":
		if strings.TrimSpace(cfg.GetString("redis.addr")) == "" {
			*errs = append(*errs, errors.New("config: production instance requires redis.addr"))
		}
		if !cfg.GetBool("instance.state_store_required") {
			*errs = append(*errs, errors.New("config: production instance requires instance.state_store_required=true"))
		}
		if !cfg.GetBool("save_load.wal.enabled") {
			*errs = append(*errs, errors.New("config: production instance requires save_load.wal.enabled=true"))
		}
		if !cfg.GetBool("save_load.wal.required") {
			*errs = append(*errs, errors.New("config: production instance requires save_load.wal.required=true"))
		}
		if strings.ToLower(strings.TrimSpace(cfg.GetString("save_load.wal.mode"))) != "durable" {
			*errs = append(*errs, errors.New("config: production instance requires save_load.wal.mode=durable"))
		}
	case "account":
		validateProductionSecret(errs, cfg, "account.session_secret")
		validateProductionSecret(errs, cfg, "account.ops_token")
		if !cfg.GetBool("account.redis_required") {
			*errs = append(*errs, errors.New("config: production account requires account.redis_required=true"))
		}
		if strings.TrimSpace(cfg.GetString("redis.addr")) == "" {
			*errs = append(*errs, errors.New("config: production account requires redis.addr"))
		}
	case "platform":
		validateProductionSecret(errs, cfg, "platform.session_secret")
		validateProductionSecret(errs, cfg, "platform.payment_secret")
	case "match_group":
		if !cfg.GetBool("match_group.redis_required") {
			*errs = append(*errs, errors.New("config: production match_group requires match_group.redis_required=true"))
		}
		if strings.TrimSpace(cfg.GetString("redis.addr")) == "" {
			*errs = append(*errs, errors.New("config: production match_group requires redis.addr"))
		}
	case "global":
		if !cfg.GetBool("global.redis_required") {
			*errs = append(*errs, errors.New("config: production global requires global.redis_required=true"))
		}
		if strings.TrimSpace(cfg.GetString("redis.addr")) == "" {
			*errs = append(*errs, errors.New("config: production global requires redis.addr"))
		}
	case "admin_gateway":
		validateProductionAdminGateway(errs, cfg)
	}
}

func validateProductionAdminGateway(errs *[]error, cfg *viper.Viper) {
	var raw struct {
		DefaultOpsAdminToken string `mapstructure:"default_ops_admin_token"`
		Tokens               []struct {
			Token string `mapstructure:"token"`
		} `mapstructure:"tokens"`
		Targets []struct {
			OpsAddr      string `mapstructure:"ops_addr"`
			OpsToken     string `mapstructure:"ops_token"`
			DispatchMode string `mapstructure:"dispatch_mode"`
		} `mapstructure:"targets"`
	}
	if err := cfg.UnmarshalKey("admin_gateway", &raw); err != nil {
		*errs = append(*errs, fmt.Errorf("config: admin_gateway production config invalid: %w", err))
		return
	}
	if len(raw.Tokens) == 0 {
		*errs = append(*errs, errors.New("config: production admin_gateway requires admin_gateway.tokens"))
	}
	for i, token := range raw.Tokens {
		if isUnsafeProductionSecret(token.Token) {
			*errs = append(*errs, fmt.Errorf("config: production admin_gateway requires non-dev admin_gateway.tokens[%d].token", i))
		}
	}
	needsDefaultOpsToken := false
	for _, target := range raw.Targets {
		mode := strings.ToLower(strings.TrimSpace(target.DispatchMode))
		if mode == "" || mode == "local_ops" {
			if strings.TrimSpace(target.OpsAddr) != "" && strings.TrimSpace(target.OpsToken) == "" {
				needsDefaultOpsToken = true
				break
			}
		}
	}
	if needsDefaultOpsToken && isUnsafeProductionSecret(raw.DefaultOpsAdminToken) {
		*errs = append(*errs, errors.New("config: production admin_gateway requires admin_gateway.default_ops_admin_token for local_ops targets without ops_token"))
	}
	if strings.TrimSpace(raw.DefaultOpsAdminToken) != "" && hasDevTokenPrefix(raw.DefaultOpsAdminToken) {
		*errs = append(*errs, errors.New("config: production admin_gateway must not use a dev admin_gateway.default_ops_admin_token"))
	}
}

func validateProductionSecret(errs *[]error, cfg *viper.Viper, key string) {
	if isUnsafeProductionSecret(cfg.GetString(key)) {
		*errs = append(*errs, fmt.Errorf("config: production requires non-dev %s", key))
	}
}

func isProductionServiceConfig(cfg *viper.Viper) bool {
	if cfg == nil {
		return false
	}
	for _, key := range []string{"env", "app.env", "environment"} {
		switch strings.ToLower(strings.TrimSpace(cfg.GetString(key))) {
		case "prod", "production":
			return true
		}
	}
	return false
}

func isUnsafeProductionSecret(value string) bool {
	return strings.TrimSpace(value) == "" || hasDevTokenPrefix(value)
}

func hasDevTokenPrefix(value string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(value)), "dev-")
}

func validateTransport(errs *[]error, cfg *viper.Viper, key string, allowed ...string) {
	value := strings.ToLower(strings.TrimSpace(cfg.GetString(key)))
	if value == "" {
		return
	}
	for _, item := range allowed {
		if value == item {
			return
		}
	}
	*errs = append(*errs, fmt.Errorf("config: %s has unsupported transport %q", key, value))
}

func isJetStreamTransport(value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	return value == "jetstream" || value == "js"
}

func validateDurationIfSet(errs *[]error, cfg *viper.Viper, key string) {
	if !cfg.IsSet(key) {
		return
	}
	if cfg.GetDuration(key) <= 0 {
		*errs = append(*errs, fmt.Errorf("config: %s must be positive", key))
	}
}

func validatePositiveDurationIfSet(errs *[]error, cfg *viper.Viper, key string) {
	validateDurationIfSet(errs, cfg, key)
}

func validatePositiveIntIfSet(errs *[]error, cfg *viper.Viper, key string) {
	if !cfg.IsSet(key) {
		return
	}
	if cfg.GetInt(key) <= 0 {
		*errs = append(*errs, fmt.Errorf("config: %s must be positive", key))
	}
}

func validateNonNegativeIntIfSet(errs *[]error, cfg *viper.Viper, key string) {
	if !cfg.IsSet(key) {
		return
	}
	if cfg.GetInt(key) < 0 {
		*errs = append(*errs, fmt.Errorf("config: %s must be non-negative", key))
	}
}

func validateNonNegativeInt64IfSet(errs *[]error, cfg *viper.Viper, key string) {
	if !cfg.IsSet(key) {
		return
	}
	if cfg.GetInt64(key) < 0 {
		*errs = append(*errs, fmt.Errorf("config: %s must be non-negative", key))
	}
}
