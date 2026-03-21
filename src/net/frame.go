package netx

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const (
	defaultNetworkTimeout = 5
	frameHeaderSize       = 4
	maxFrameSize          = 64 << 20
)

type requestEnvelope struct {
	Path    string         `json:"path"`
	Payload map[string]any `json:"payload"`
}

type responseEnvelope struct {
	Payload map[string]any `json:"payload"`
}

func normalizePayload(payload any) (map[string]any, error) {
	if payload == nil {
		return map[string]any{}, nil
	}
	if typed, ok := payload.(map[string]any); ok {
		return typed, nil
	}

	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}
	var result map[string]any
	if err := json.Unmarshal(encoded, &result); err != nil {
		return nil, fmt.Errorf("payload must encode as a JSON object: %w", err)
	}
	if result == nil {
		result = map[string]any{}
	}
	return result, nil
}

func normalizePath(path string) string {
	if path == "" {
		return "/"
	}
	if path[0] != '/' {
		return "/" + path
	}
	return path
}

func writeFrame(writer *bufio.Writer, payload []byte) error {
	if len(payload) > maxFrameSize {
		return fmt.Errorf("payload too large: %d bytes", len(payload))
	}

	var header [frameHeaderSize]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))

	if _, err := writer.Write(header[:]); err != nil {
		return err
	}
	if _, err := writer.Write(payload); err != nil {
		return err
	}
	if _, err := writer.Write(header[:]); err != nil {
		return err
	}
	return writer.Flush()
}

func readFrame(reader *bufio.Reader) ([]byte, error) {
	var header [frameHeaderSize]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return nil, err
	}

	size := binary.BigEndian.Uint32(header[:])
	if size > maxFrameSize {
		return nil, fmt.Errorf("frame too large: %d bytes", size)
	}

	payload := make([]byte, size)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return nil, err
	}

	var marker [frameHeaderSize]byte
	if _, err := io.ReadFull(reader, marker[:]); err != nil {
		return nil, err
	}
	if marker != header {
		return nil, errors.New("invalid frame marker")
	}

	return payload, nil
}
