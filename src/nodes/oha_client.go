package nodes

import (
	"aegean/common"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
)

type OHAClient struct {
	*Client
	mu         sync.Mutex
	finished   bool
	requestSeq uint64
}

func NewOHAClient(name, host string, port int, next []string, requestLogic func(c *Client)) *OHAClient {
	if requestLogic == nil {
		panic("oha client requires RequestLogic")
	}
	baseClient := NewClient(name, host, port, next, requestLogic)
	client := &OHAClient{
		Client: baseClient,
	}
	client.Node.HandleMessage = client.HandleMessage
	client.Node.HandleProgress = client.HandleProgress
	client.Node.HandleReady = client.HandleReady
	return client
}

func (c *OHAClient) Start() {
	go func() {
		c.WaitForNodesReady([]string{c.Name})
		c.RequestLogic(c.Client)
		c.mu.Lock()
		c.finished = true
		c.mu.Unlock()
	}()
	c.Node.Start()
}

func (c *OHAClient) HandleMessage(payload map[string]any) map[string]any {
	requestID := atomic.AddUint64(&c.requestSeq, 1)

	outgoing := make(map[string]any, len(payload)+4)
	for k, v := range payload {
		outgoing[k] = v
	}
	outgoing["type"] = "request"
	outgoing["request_id"] = requestID
	outgoing["sender"] = c.Name

	type sendResult struct {
		node     string
		response map[string]any
		err      error
	}

	results := make(chan sendResult, len(c.Next))
	for _, nextNode := range c.Next {
		go func(target string) {
			response, err := common.SendMessage(target, 8000, outgoing)
			results <- sendResult{node: target, response: response, err: err}
		}(nextNode)
	}

	quorumSize := len(c.Next)/2 + 1
	responders := make(map[string]struct{}, len(c.Next))
	var quorumResponse map[string]any
	var lastError error

	for i := 0; i < len(c.Next); i++ {
		result := <-results
		if result.err != nil {
			lastError = result.err
			continue
		}

		sender, _ := result.response["sender"].(string)
		if sender == "" {
			sender = result.node
		}
		if _, seen := responders[sender]; seen {
			continue
		}
		responders[sender] = struct{}{}
		if quorumResponse == nil {
			quorumResponse = result.response
		}
		if len(responders) >= quorumSize {
			return quorumResponse
		}
	}

	log.Printf("warning: oha quorum not reached for request_id=%v responders=%d quorum=%d last_error=%v", requestID, len(responders), quorumSize, lastError)
	return map[string]any{
		"status":     "error",
		"error":      "quorum_not_reached",
		"request_id": requestID,
		"responders": len(responders),
		"quorum":     quorumSize,
		"detail":     fmt.Sprintf("%v", lastError),
	}
}

func (c *OHAClient) HandleProgress(payload map[string]any) map[string]any {
	c.mu.Lock()
	finished := c.finished
	c.mu.Unlock()

	progress := float32(0)
	if finished {
		progress = 1
	}

	return map[string]any{
		"progress":               progress,
		"finished":               finished,
		"disableProgressTimeout": true,
	}
}

func (c *OHAClient) HandleReady(payload map[string]any) map[string]any {
	return map[string]any{
		"ready": true,
	}
}
