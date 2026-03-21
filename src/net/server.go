package netx

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	stdnet "net"
	"net/http"
	"time"
)

type MessageHandler func(request map[string]any) map[string]any

func MakeHandler(messageHandler MessageHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var request map[string]any
		if r.ContentLength > 0 {
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				http.Error(w, "Invalid JSON", http.StatusBadRequest)
				return
			}
		} else {
			request = make(map[string]any)
		}

		response := messageHandler(request)
		if response == nil {
			response = map[string]any{}
		}

		body, err := json.Marshal(response)
		if err != nil {
			http.Error(w, "Failed to encode response", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", string(rune(len(body))))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}
}

func ServeTCP(listener stdnet.Listener, handlers map[string]MessageHandler) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, stdnet.ErrClosed) {
				return nil
			}
			if ne, ok := err.(stdnet.Error); ok && ne.Temporary() {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			return err
		}

		tcpConn, ok := conn.(*stdnet.TCPConn)
		if !ok {
			_ = conn.Close()
			continue
		}
		if err := configureTCPConn(tcpConn); err != nil {
			_ = tcpConn.Close()
			continue
		}

		go serveTCPConn(tcpConn, handlers)
	}
}

func serveTCPConn(conn *stdnet.TCPConn, handlers map[string]MessageHandler) {
	defer conn.Close()

	reader := bufio.NewReaderSize(conn, 64<<10)
	prefix, err := reader.Peek(4)
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, stdnet.ErrClosed) {
			return
		}
		return
	}
	if looksLikeHTTP(prefix) {
		serveHTTPConn(conn, reader, handlers)
		return
	}

	writer := bufio.NewWriterSize(conn, 64<<10)
	for {
		requestBytes, err := readFrame(reader)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, stdnet.ErrClosed) {
				return
			}
			return
		}

		var request requestEnvelope
		if err := json.Unmarshal(requestBytes, &request); err != nil {
			if writeErr := writeEnvelope(writer, map[string]any{
				"status": "error",
				"error":  "invalid json",
			}); writeErr != nil {
				return
			}
			continue
		}

		handler := handlers[normalizePath(request.Path)]
		if handler == nil {
			handler = handlers["/"]
		}
		if handler == nil {
			if writeErr := writeEnvelope(writer, map[string]any{
				"status": "error",
				"error":  "handler not found",
			}); writeErr != nil {
				return
			}
			continue
		}

		response := handler(request.Payload)
		if response == nil {
			response = map[string]any{}
		}
		if err := writeEnvelope(writer, response); err != nil {
			return
		}
	}
}

func writeEnvelope(writer *bufio.Writer, payload map[string]any) error {
	body, err := json.Marshal(responseEnvelope{Payload: payload})
	if err != nil {
		body, _ = json.Marshal(responseEnvelope{Payload: map[string]any{
			"status": "error",
			"error":  "failed to encode response",
		}})
	}
	return writeFrame(writer, body)
}
