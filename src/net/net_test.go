package netx

import (
	"encoding/json"
	"fmt"
	stdnet "net"
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
	listener, err := stdnet.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer listener.Close()

	go func() {
		_ = ServeTCP(listener, map[string]MessageHandler{
			"/": func(req map[string]any) map[string]any {
				return map[string]any{"received": req["data"]}
			},
		})
	}()

	addr := listener.Addr().(*stdnet.TCPAddr)
	host := addr.IP.String()
	port := addr.Port

	result, err := SendMessage(host, port, map[string]any{"data": "test"}, 1, time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["received"] != "test" {
		t.Errorf("expected received=test, got %v", result["received"])
	}
}

func TestServeTCPAcceptsHTTPPost(t *testing.T) {
	listener, err := stdnet.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer listener.Close()

	go func() {
		_ = ServeTCP(listener, map[string]MessageHandler{
			"/ready": func(req map[string]any) map[string]any {
				return map[string]any{"ready": true}
			},
		})
	}()

	resp, err := http.Post(
		fmt.Sprintf("http://%s/ready", listener.Addr().String()),
		"application/json",
		strings.NewReader(`{}`),
	)
	if err != nil {
		t.Fatalf("http post failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if ready, _ := payload["ready"].(bool); !ready {
		t.Fatalf("expected ready=true, got %v", payload)
	}
}
