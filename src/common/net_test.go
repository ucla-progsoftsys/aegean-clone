package common

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMakeHandler(t *testing.T) {
	handler := MakeHandler(func(req map[string]any) map[string]any {
		return map[string]any{"echo": req["msg"]}
	})

	t.Run("valid POST request", func(t *testing.T) {
		body := strings.NewReader(`{"msg":"hello"}`)
		req := httptest.NewRequest(http.MethodPost, "/", body)
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		handler(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		var resp map[string]any
		json.NewDecoder(w.Body).Decode(&resp)
		if resp["echo"] != "hello" {
			t.Errorf("expected echo=hello, got %v", resp["echo"])
		}
	})

	t.Run("rejects non-POST", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		w := httptest.NewRecorder()

		handler(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("expected status 405, got %d", w.Code)
		}
	})
}

func TestSendMessage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		json.NewDecoder(r.Body).Decode(&req)
		resp := map[string]any{"received": req["data"]}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	addr := server.Listener.Addr().String()
	parts := strings.Split(addr, ":")
	host := parts[0]
	var port int
	fmt.Sscanf(parts[1], "%d", &port)

	result, err := SendMessage(host, port, map[string]any{"data": "test"}, 1, time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["received"] != "test" {
		t.Errorf("expected received=test, got %v", result["received"])
	}
}
