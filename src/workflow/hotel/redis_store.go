package hotelworkflow

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
	hotelRedisOnce   sync.Once
	hotelRedisClient *redis.Client
	hotelRedisErr    error
)

func hotelRedisEnabled(runConfig map[string]any) bool {
	if os.Getenv("HOTEL_REDIS_ENABLE") == "1" {
		return true
	}
	return common.BoolOrDefault(runConfig, "hotel_redis_enable", false)
}

func hotelRedisAddr(runConfig map[string]any) string {
	if addr := os.Getenv("HOTEL_REDIS_ADDR"); addr != "" {
		return addr
	}
	if configured, ok := runConfig["hotel_redis_addr"]; ok {
		addr, ok := configured.(string)
		if !ok {
			panic("run config field \"hotel_redis_addr\" must be a string")
		}
		if addr != "" {
			return addr
		}
	}
	return "127.0.0.1:6379"
}

func getHotelRedisClient(runConfig map[string]any) (*redis.Client, error) {
	if !hotelRedisEnabled(runConfig) {
		return nil, nil
	}
	hotelRedisOnce.Do(func() {
		client := redis.NewClient(&redis.Options{
			Addr:         hotelRedisAddr(runConfig),
			DialTimeout:  500 * time.Millisecond,
			ReadTimeout:  500 * time.Millisecond,
			WriteTimeout: 500 * time.Millisecond,
		})
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := client.Ping(ctx).Err(); err != nil {
			hotelRedisErr = err
			_ = client.Close()
			return
		}
		hotelRedisClient = client
	})
	return hotelRedisClient, hotelRedisErr
}

func hotelReadKV(e *exec.Exec, key string) string {
	if value := e.ReadKV(key); value != "" {
		return value
	}
	client, err := getHotelRedisClient(e.RunConfig)
	if err != nil {
		log.Printf("hotel redis read unavailable for %s: %v", key, err)
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
		log.Printf("hotel redis read failed for %s: %v", key, err)
		return ""
	}
}

func hotelWriteKV(e *exec.Exec, key, value string) {
	e.WriteKV(key, value)
	client, err := getHotelRedisClient(e.RunConfig)
	if err != nil {
		log.Printf("hotel redis write unavailable for %s: %v", key, err)
		return
	}
	if client == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Set(ctx, key, value, 0).Err(); err != nil {
		log.Printf("hotel redis write failed for %s: %v", key, err)
	}
}

func hotelPersistStateSeed(e *exec.Exec, state map[string]string) {
	client, err := getHotelRedisClient(e.RunConfig)
	if err != nil {
		log.Printf("hotel redis seed unavailable: %v", err)
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
		log.Printf("hotel redis seed failed: %v", err)
	}
}
