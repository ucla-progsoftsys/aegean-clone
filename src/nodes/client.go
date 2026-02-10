package nodes

import (
	"fmt"
	"log"
	"sync"
)

type Client struct {
	*Node
	Next              []string
	completedRequests map[string]struct{}
	mu                sync.Mutex
	cond              *sync.Cond
	RequestLogic      func(c *Client)
}

func NewClient(name, host string, port int, next []string, requestLogic func(c *Client)) *Client {
	if requestLogic == nil {
		log.Fatalf("client requires RequestLogic")
	}
	client := &Client{
		Node:              NewNode(name, host, port),
		Next:              next,
		completedRequests: make(map[string]struct{}),
		RequestLogic:      requestLogic,
	}
	client.cond = sync.NewCond(&client.mu)
	client.Node.HandleMessage = client.HandleMessage
	return client
}

func (c *Client) Start() {
	go c.RequestLogic(c)
	c.Node.Start()
}

func (c *Client) HandleMessage(payload map[string]any) map[string]any {
	log.Printf("Handler called on %s with payload: %v", c.Name, payload)

	requestID := payload["request_id"]
	response, _ := payload["response"].(map[string]any)
	sender, _ := payload["sender"].(string)
	key := toKey(requestID)

	logger := GetClientLogger()

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, done := c.completedRequests[key]; done {
		log.Printf("Client %s: Ignoring duplicate response for %v", c.Name, requestID)
		logger.LogResponse(requestID, sender, payload, response)
		return map[string]any{"status": "already_completed"}
	}

	// In CFT mode with a single exec pipeline, one response is sufficient
	log.Printf("Client %s: Received response for request %v: %v", c.Name, requestID, response)
	c.completedRequests[key] = struct{}{}
	c.cond.Broadcast()
	// TODO: In full BFT mode, would wait for f+1 matching responses
	log.Printf("Client %s: Request %v completed with: %v", c.Name, requestID, response)

	logger.LogResponse(requestID, sender, payload, response)

	return map[string]any{"status": "response_received", "request_id": requestID}
}

func toKey(value any) string {
	return fmt.Sprintf("%v", value)
}

func (c *Client) WaitForRequestCompletion(requestID any) {
	key := toKey(requestID)

	c.mu.Lock()
	defer c.mu.Unlock()

	for {
		if _, done := c.completedRequests[key]; done {
			return
		}
		c.cond.Wait()
	}
}
