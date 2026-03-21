package netx

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	stdnet "net"
	"net/http"
)

func looksLikeHTTP(prefix []byte) bool {
	if len(prefix) < 4 {
		return false
	}
	return bytes.Equal(prefix, []byte("POST")) ||
		bytes.Equal(prefix, []byte("GET ")) ||
		bytes.Equal(prefix, []byte("HEAD")) ||
		bytes.Equal(prefix, []byte("PUT ")) ||
		bytes.Equal(prefix, []byte("OPTI"))
}

func serveHTTPConn(conn *stdnet.TCPConn, reader *bufio.Reader, handlers map[string]MessageHandler) {
	defer conn.Close()

	writer := bufio.NewWriterSize(conn, 64<<10)
	for {
		req, err := http.ReadRequest(reader)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, stdnet.ErrClosed) {
				return
			}
			return
		}

		responsePayload, statusCode := handleHTTPRequest(req, handlers)
		_ = req.Body.Close()

		body, err := json.Marshal(responsePayload)
		if err != nil {
			body = []byte(`{"status":"error","error":"failed to encode response"}`)
			statusCode = http.StatusInternalServerError
		}

		resp := &http.Response{
			StatusCode:    statusCode,
			Status:        fmt.Sprintf("%d %s", statusCode, http.StatusText(statusCode)),
			ProtoMajor:    1,
			ProtoMinor:    1,
			ContentLength: int64(len(body)),
			Header:        make(http.Header),
			Body:          io.NopCloser(bytes.NewReader(body)),
			Request:       req,
		}
		resp.Header.Set("Content-Type", "application/json")
		if req.Close {
			resp.Close = true
			resp.Header.Set("Connection", "close")
		}

		if err := resp.Write(writer); err != nil {
			return
		}
		if err := writer.Flush(); err != nil {
			return
		}
		if req.Close {
			return
		}
	}
}

func handleHTTPRequest(req *http.Request, handlers map[string]MessageHandler) (map[string]any, int) {
	if req.Method != http.MethodPost {
		return map[string]any{
			"status": "error",
			"error":  "method not allowed",
		}, http.StatusMethodNotAllowed
	}

	var request map[string]any
	if req.ContentLength > 0 {
		if err := json.NewDecoder(req.Body).Decode(&request); err != nil {
			return map[string]any{
				"status": "error",
				"error":  "invalid json",
			}, http.StatusBadRequest
		}
	} else {
		request = map[string]any{}
	}

	handler := handlers[normalizePath(req.URL.Path)]
	if handler == nil {
		handler = handlers["/"]
	}
	if handler == nil {
		return map[string]any{
			"status": "error",
			"error":  "handler not found",
		}, http.StatusNotFound
	}

	response := handler(request)
	if response == nil {
		response = map[string]any{}
	}
	return response, http.StatusOK
}
