package mediaworkflow

import (
	"aegean/common"
	"aegean/components/exec"
	"context"
	"log"
	"os"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	mediaRedisMu     sync.Mutex
	mediaRedisClient *redis.Client
	mediaRedisAddrID string
)

func mediaRedisEnabled(runConfig map[string]any) bool {
	if enabled, ok := os.LookupEnv("MEDIA_REDIS_ENABLE"); ok {
		return enabled == "1"
	}
	return common.BoolOrDefault(runConfig, "media_redis_enable", false)
}

func mediaRedisAddr(runConfig map[string]any) string {
	if addr := os.Getenv("MEDIA_REDIS_ADDR"); addr != "" {
		return addr
	}
	if configured, ok := runConfig["media_redis_addr"]; ok {
		addr, ok := configured.(string)
		if !ok {
			panic("run config field \"media_redis_addr\" must be a string")
		}
		if addr != "" {
			return addr
		}
	}
	return "127.0.0.1:6379"
}

func getMediaRedisClient(runConfig map[string]any) (*redis.Client, error) {
	if !mediaRedisEnabled(runConfig) {
		return nil, nil
	}
	addr := mediaRedisAddr(runConfig)

	mediaRedisMu.Lock()
	defer mediaRedisMu.Unlock()

	if mediaRedisClient != nil && mediaRedisAddrID == addr {
		return mediaRedisClient, nil
	}
	if mediaRedisClient != nil {
		_ = mediaRedisClient.Close()
		mediaRedisClient = nil
		mediaRedisAddrID = ""
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

	mediaRedisClient = client
	mediaRedisAddrID = addr
	return mediaRedisClient, nil
}

func mediaReadKV(e *exec.Exec, key string) string {
	if value := e.ReadKV(key); value != "" {
		return value
	}
	client, err := getMediaRedisClient(e.RunConfig)
	if err != nil {
		log.Printf("media redis read unavailable for %s: %v", key, err)
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
		log.Printf("media redis read failed for %s: %v", key, err)
		return ""
	}
}

func mediaWriteKV(e *exec.Exec, key, value string) {
	e.WriteKV(key, value)
	client, err := getMediaRedisClient(e.RunConfig)
	if err != nil {
		log.Printf("media redis write unavailable for %s: %v", key, err)
		return
	}
	if client == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Set(ctx, key, value, 0).Err(); err != nil {
		log.Printf("media redis write failed for %s: %v", key, err)
	}
}

func mediaPersistStateSeed(e *exec.Exec, state map[string]string) {
	client, err := getMediaRedisClient(e.RunConfig)
	if err != nil {
		log.Printf("media redis seed unavailable: %v", err)
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
		log.Printf("media redis seed failed: %v", err)
	}
}
