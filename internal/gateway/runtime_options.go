package gateway

import (
	"fmt"
	"os"
	"strconv"
)

func semanticCacheEnabled() (bool, error) {
	raw := os.Getenv("GW_CACHE_ENABLED")
	if raw == "" {
		return true, nil
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("GW_CACHE_ENABLED must be a boolean")
	}
	return v, nil
}

func poolEnv(name string, fallback, minimum int) (int, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < minimum || v > 1024 {
		return 0, fmt.Errorf("%s must be %d..1024", name, minimum)
	}
	return v, nil
}

func mysqlPoolOptions() (open, idle int, err error) {
	open, err = poolEnv("GW_MYSQL_MAX_OPEN_CONNS", 30, 2)
	if err != nil {
		return
	}
	idle, err = poolEnv("GW_MYSQL_MAX_IDLE_CONNS", 10, 0)
	if err == nil && idle > open {
		err = fmt.Errorf("GW_MYSQL_MAX_IDLE_CONNS must not exceed GW_MYSQL_MAX_OPEN_CONNS")
	}
	return
}
