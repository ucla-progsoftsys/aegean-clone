package main

import (
	"log"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"sync"
	"syscall"
)

func composeCleanup(cleanups ...func()) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			for i := len(cleanups) - 1; i >= 0; i-- {
				if cleanups[i] != nil {
					cleanups[i]()
				}
			}
		})
	}
}

func startCPUProfilingFromEnv() func() {
	profilePath := os.Getenv("AEGEAN_CPU_PROFILE_PATH")
	if profilePath == "" {
		return func() {}
	}

	f, err := os.Create(profilePath)
	if err != nil {
		log.Printf("cpu profiling disabled: create %s: %v", profilePath, err)
		return func() {}
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		log.Printf("cpu profiling disabled: start profile %s: %v", profilePath, err)
		_ = f.Close()
		return func() {}
	}

	log.Printf("cpu profiling enabled: %s", profilePath)
	return composeCleanup(func() {
		pprof.StopCPUProfile()
		if err := f.Close(); err != nil {
			log.Printf("cpu profiling close failed: %v", err)
		}
	})
}

func startBlockProfilingFromEnv() func() {
	profilePath := os.Getenv("AEGEAN_BLOCK_PROFILE_PATH")
	if profilePath == "" {
		return func() {}
	}

	runtime.SetBlockProfileRate(1)
	log.Printf("block profiling enabled: %s", profilePath)
	return composeCleanup(func() {
		defer runtime.SetBlockProfileRate(0)
		f, err := os.Create(profilePath)
		if err != nil {
			log.Printf("block profiling disabled: create %s: %v", profilePath, err)
			return
		}
		defer f.Close()
		if err := pprof.Lookup("block").WriteTo(f, 0); err != nil {
			log.Printf("block profiling write failed: %v", err)
		}
	})
}

func startMutexProfilingFromEnv() func() {
	profilePath := os.Getenv("AEGEAN_MUTEX_PROFILE_PATH")
	if profilePath == "" {
		return func() {}
	}

	runtime.SetMutexProfileFraction(1)
	log.Printf("mutex profiling enabled: %s", profilePath)
	return composeCleanup(func() {
		defer runtime.SetMutexProfileFraction(0)
		f, err := os.Create(profilePath)
		if err != nil {
			log.Printf("mutex profiling disabled: create %s: %v", profilePath, err)
			return
		}
		defer f.Close()
		if err := pprof.Lookup("mutex").WriteTo(f, 0); err != nil {
			log.Printf("mutex profiling write failed: %v", err)
		}
	})
}

func startProfilingFromEnv() func() {
	return composeCleanup(
		startCPUProfilingFromEnv(),
		startBlockProfilingFromEnv(),
		startMutexProfilingFromEnv(),
	)
}

func installSignalCleanup(cleanup func()) {
	if cleanup == nil {
		return
	}
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		cleanup()
		os.Exit(0)
	}()
}
