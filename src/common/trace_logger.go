package common

import (
	"encoding/json"
	"os"
	"sync"
)

// TraceLogger writes one JSON-encoded trace event per line.
type TraceLogger struct {
	file *os.File
	mu   sync.Mutex
}

// NewNoopTraceLogger creates a logger that drops all events.
func NewNoopTraceLogger() *TraceLogger {
	return &TraceLogger{}
}

// NewTraceLogger creates a trace logger backed by filename.
func NewTraceLogger(filename string) (*TraceLogger, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}
	return &TraceLogger{file: file}, nil
}

// WriteTrace appends one trace event as a JSON line.
func (l *TraceLogger) WriteTrace(event map[string]any) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return nil
	}

	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if _, err := l.file.Write(data); err != nil {
		return err
	}
	if _, err := l.file.Write([]byte("\n")); err != nil {
		return err
	}
	return nil
}

// Close closes the underlying file.
func (l *TraceLogger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file = nil
	return err
}
