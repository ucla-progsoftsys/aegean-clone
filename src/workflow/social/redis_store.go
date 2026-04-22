package socialworkflow

import (
	"aegean/components/exec"
	"context"
	"log"
	"os"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	socialRedisMu     sync.Mutex
	socialRedisClient *redis.Client
	socialRedisAddrID string
)

func socialRedisEnabled() bool {
	if enabled, ok := os.LookupEnv("SOCIAL_REDIS_ENABLE"); ok {
		return enabled == "1"
	}
	return false
}

func socialRedisAddr() string {
	if addr := os.Getenv("SOCIAL_REDIS_ADDR"); addr != "" {
		return addr
	}
	return "127.0.0.1:6379"
}

func getSocialRedisClient() (*redis.Client, error) {
	if !socialRedisEnabled() {
		return nil, nil
	}
	addr := socialRedisAddr()

	socialRedisMu.Lock()
	defer socialRedisMu.Unlock()

	if socialRedisClient != nil && socialRedisAddrID == addr {
		return socialRedisClient, nil
	}
	if socialRedisClient != nil {
		_ = socialRedisClient.Close()
		socialRedisClient = nil
		socialRedisAddrID = ""
	}

	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		DialTimeout:  500 * time.Millisecond,
		ReadTimeout:  500 * time.Millisecond,
		WriteTimeout: 500 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, err
	}

	socialRedisClient = client
	socialRedisAddrID = addr
	return socialRedisClient, nil
}

func socialReadKV(e *exec.Exec, key string) string {
	if value := e.ReadKV(key); value != "" {
		return value
	}
	client, err := getSocialRedisClient()
	if err != nil {
		log.Printf("social redis read unavailable for %s: %v", key, err)
		return ""
	}
	if client == nil {
		return ""
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	value, err := client.Get(ctx, key).Result()
	switch err {
	case nil:
		return value
	case redis.Nil:
		return ""
	default:
		log.Printf("social redis read failed for %s: %v", key, err)
		return ""
	}
}

func socialWriteKV(e *exec.Exec, key, value string) {
	e.WriteKV(key, value)
	client, err := getSocialRedisClient()
	if err != nil {
		log.Printf("social redis write unavailable for %s: %v", key, err)
		return
	}
	if client == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Set(ctx, key, value, 0).Err(); err != nil {
		log.Printf("social redis write failed for %s: %v", key, err)
	}
}

func socialPersistStateSeed(state map[string]string) {
	client, err := getSocialRedisClient()
	if err != nil {
		log.Printf("social redis seed unavailable: %v", err)
		return
	}
	if client == nil || len(state) == 0 {
		return
	}
	values := make(map[string]any, len(state))
	for key, value := range state {
		values[key] = value
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.MSet(ctx, values).Err(); err != nil {
		log.Printf("social redis seed failed: %v", err)
	}
}
