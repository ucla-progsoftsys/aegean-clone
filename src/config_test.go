package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadRunConfigResolvesArchitectureSiblingToRunsDir(t *testing.T) {
	root := t.TempDir()
	architectureDir := filepath.Join(root, "experiment", "architecture")
	runDir := filepath.Join(root, "experiment", "runs", "basic_oha_large_req")

	if err := os.MkdirAll(architectureDir, 0o755); err != nil {
		t.Fatalf("mkdir architecture dir: %v", err)
	}
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		t.Fatalf("mkdir run dir: %v", err)
	}

	architecturePath := filepath.Join(architectureDir, "basic_oha.yaml")
	if err := os.WriteFile(architecturePath, []byte("services:\n  svc:\n    type: client\nnodes:\n  node0:\n    service: svc\n"), 0o644); err != nil {
		t.Fatalf("write architecture config: %v", err)
	}

	runConfigPath := filepath.Join(runDir, "worker_4.yaml")
	if err := os.WriteFile(runConfigPath, []byte("architecture: basic_oha.yaml\nworker_count: 4\nrun_timeout_seconds: 30\n"), 0o644); err != nil {
		t.Fatalf("write run config: %v", err)
	}

	cfg, err := loadRunConfig(runConfigPath)
	if err != nil {
		t.Fatalf("load run config: %v", err)
	}

	if cfg.Architecture != architecturePath {
		t.Fatalf("architecture path = %q, want %q", cfg.Architecture, architecturePath)
	}
	if got := cfg.Params["worker_count"]; got != 4 {
		t.Fatalf("worker_count = %#v, want 4", got)
	}
	if got := cfg.Params["run_timeout_seconds"]; got != 30 {
		t.Fatalf("run_timeout_seconds = %#v, want 30", got)
	}
}
