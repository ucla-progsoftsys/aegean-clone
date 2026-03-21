package netx

import (
	"bufio"
	"encoding/json"
	"fmt"
	stdnet "net"
	"sync"
	"time"
)

type pooledConn struct {
	addr   string
	dialer *stdnet.Dialer

	mu     sync.Mutex
	conn   *stdnet.TCPConn
	reader *bufio.Reader
	writer *bufio.Writer
}

var outboundConns sync.Map

func ResetNetworkConnections() {
	outboundConns.Range(func(_, value any) bool {
		value.(*pooledConn).invalidate()
		return true
	})
	outboundConns = sync.Map{}
}

func SendMessage(host string, port int, payload any, opts ...any) (map[string]any, error) {
	return SendMessageToPath(host, port, "/", payload, opts...)
}

func SendMessageToPath(host string, port int, path string, payload any, opts ...any) (map[string]any, error) {
	retries := 3
	timeout := 5 * time.Second
	if len(opts) >= 1 {
		retries = opts[0].(int)
	}
	if len(opts) >= 2 {
		timeout = opts[1].(time.Duration)
	}
	if retries <= 0 {
		retries = 1
	}

	requestPayload, err := normalizePayload(payload)
	if err != nil {
		return nil, err
	}

	body, err := json.Marshal(requestEnvelope{
		Path:    normalizePath(path),
		Payload: requestPayload,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	addr := stdnet.JoinHostPort(host, fmt.Sprintf("%d", port))
	client := getPooledConn(addr, timeout)

	var lastErr error
	for attempt := 0; attempt < retries; attempt++ {
		responseBytes, err := client.roundTrip(body, timeout)
		if err != nil {
			lastErr = err
			continue
		}

		var response responseEnvelope
		if len(responseBytes) > 0 {
			if err := json.Unmarshal(responseBytes, &response); err != nil {
				lastErr = fmt.Errorf("failed to decode response: %w", err)
				client.invalidate()
				continue
			}
		}

		if response.Payload == nil {
			return map[string]any{}, nil
		}
		return response.Payload, nil
	}

	return nil, lastErr
}

func getPooledConn(addr string, timeout time.Duration) *pooledConn {
	if existing, ok := outboundConns.Load(addr); ok {
		return existing.(*pooledConn)
	}

	client := &pooledConn{
		addr: addr,
		dialer: &stdnet.Dialer{
			Timeout:   timeout,
			KeepAlive: 30 * time.Second,
		},
	}
	actual, _ := outboundConns.LoadOrStore(addr, client)
	return actual.(*pooledConn)
}

func (pc *pooledConn) roundTrip(payload []byte, timeout time.Duration) ([]byte, error) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if err := pc.ensureConnLocked(timeout); err != nil {
		return nil, err
	}
	if err := pc.conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		pc.invalidateLocked()
		return nil, err
	}

	if err := writeFrame(pc.writer, payload); err != nil {
		pc.invalidateLocked()
		return nil, err
	}

	response, err := readFrame(pc.reader)
	if err != nil {
		pc.invalidateLocked()
		return nil, err
	}
	return response, nil
}

func (pc *pooledConn) ensureConnLocked(timeout time.Duration) error {
	if pc.conn != nil {
		return nil
	}

	rawConn, err := pc.dialer.Dial("tcp", pc.addr)
	if err != nil {
		return err
	}

	tcpConn, ok := rawConn.(*stdnet.TCPConn)
	if !ok {
		rawConn.Close()
		return fmt.Errorf("unexpected connection type %T", rawConn)
	}

	if err := configureTCPConn(tcpConn); err != nil {
		tcpConn.Close()
		return err
	}
	if err := tcpConn.SetDeadline(time.Now().Add(timeout)); err != nil {
		tcpConn.Close()
		return err
	}

	pc.conn = tcpConn
	pc.reader = bufio.NewReaderSize(tcpConn, 64<<10)
	pc.writer = bufio.NewWriterSize(tcpConn, 64<<10)
	return nil
}

func (pc *pooledConn) invalidate() {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.invalidateLocked()
}

func (pc *pooledConn) invalidateLocked() {
	if pc.conn != nil {
		_ = pc.conn.Close()
	}
	pc.conn = nil
	pc.reader = nil
	pc.writer = nil
}

func configureTCPConn(conn *stdnet.TCPConn) error {
	if err := conn.SetNoDelay(true); err != nil {
		return err
	}
	if err := conn.SetKeepAlive(true); err != nil {
		return err
	}
	if err := conn.SetKeepAlivePeriod(30 * time.Second); err != nil {
		return err
	}
	if err := conn.SetReadBuffer(256 << 10); err != nil {
		return err
	}
	if err := conn.SetWriteBuffer(256 << 10); err != nil {
		return err
	}
	return nil
}
