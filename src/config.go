package main

import (
	"log"
	"strconv"
	"time"

	"aegean/common"
	"aegean/nodes"
)

type NodeConfig struct {
	Type             string
	Next             []string
	Clients          []string
	Shim             string
	Verifiers        []string
	Peers            []string
	Execs            []string
	IsPrimaryBatcher bool
	ExecWorkflow     string
	ClientWorkflow   string
	ResponseWorkflow string
}

var config = map[string]NodeConfig{
	"node1": {Type: "client", Next: []string{"node4", "node5", "node6"}, ClientWorkflow: "default"},
	"node2": {Type: "client", Next: []string{"node4", "node5", "node6"}, ClientWorkflow: "default"},
	"node3": {Type: "client", Next: []string{"node4", "node5", "node6"}, ClientWorkflow: "default"},
	"node4": {
		Type:             "server",
		Clients:          []string{"node1", "node2", "node3"},
		Verifiers:        []string{"node4", "node5", "node6"},
		Execs:            []string{"node4", "node5", "node6"},
		Peers:            []string{"node5", "node6"},
		IsPrimaryBatcher: true,
		ExecWorkflow:     "fanout",
		ResponseWorkflow: "forward_to_clients",
	},
	"node5": {
		Type:             "server",
		Clients:          []string{"node1", "node2", "node3"},
		Verifiers:        []string{"node4", "node5", "node6"},
		Execs:            []string{"node4", "node5", "node6"},
		Peers:            []string{"node4", "node6"},
		IsPrimaryBatcher: false,
		ExecWorkflow:     "fanout",
		ResponseWorkflow: "forward_to_clients",
	},
	"node6": {
		Type:             "server",
		Clients:          []string{"node1", "node2", "node3"},
		Verifiers:        []string{"node4", "node5", "node6"},
		Execs:            []string{"node4", "node5", "node6"},
		Peers:            []string{"node4", "node5"},
		IsPrimaryBatcher: false,
		ExecWorkflow:     "fanout",
		ResponseWorkflow: "forward_to_clients",
	},
	"node7": {
		Type:             "server",
		Clients:          []string{"node4", "node5", "node6"},
		Verifiers:        []string{"node7", "node8", "node9"},
		Execs:            []string{"node7", "node8", "node9"},
		Peers:            []string{"node8", "node9"},
		IsPrimaryBatcher: true,
		ExecWorkflow:     "default",
		ResponseWorkflow: "noop",
	},
	"node8": {
		Type:             "server",
		Clients:          []string{"node4", "node5", "node6"},
		Verifiers:        []string{"node7", "node8", "node9"},
		Execs:            []string{"node7", "node8", "node9"},
		Peers:            []string{"node7", "node9"},
		IsPrimaryBatcher: false,
		ExecWorkflow:     "default",
		ResponseWorkflow: "noop",
	},
	"node9": {
		Type:             "server",
		Clients:          []string{"node4", "node5", "node6"},
		Verifiers:        []string{"node7", "node8", "node9"},
		Execs:            []string{"node7", "node8", "node9"},
		Peers:            []string{"node7", "node8"},
		IsPrimaryBatcher: false,
		ExecWorkflow:     "default",
		ResponseWorkflow: "noop",
	},
}

func init() {
	clientWorkflows["default"] = ClientRequestLogic
	execWorkflows["default"] = ExecuteRequest
	execWorkflows["fanout"] = ExecuteRequestFanout
	responseWorkflows["default"] = ResponseForwardToClients
	responseWorkflows["forward_to_clients"] = ResponseForwardToClients
	responseWorkflows["noop"] = ResponseNoop
}

var clientWorkflows = map[string]func(c *nodes.Client){}
var execWorkflows = map[string]nodes.ExecuteRequestFunc{}
var responseWorkflows = map[string]nodes.ExecuteResponseFunc{}
var responseQuorum = common.NewQuorumHelper(2)

func ClientRequestLogic(c *nodes.Client) {
	// Wait for other nodes to be turned on. TODO: improvable
	time.Sleep(2 * time.Second)

	logger := nodes.GetClientLogger()

	for requestID := 1; requestID <= 10; requestID++ {
		timestamp := float64(time.Now().UnixNano()) / 1e9

		request := map[string]any{
			"request_id": requestID,
			"timestamp":  timestamp,
			"sender":     c.Name,
			"op":         "spin_write_read",
			"op_payload": map[string]any{
				"spin_time":   0.1,
				"write_key":   "1",
				"write_value": "value_" + strconv.Itoa(requestID),
				"read_key":    "1",
			},
		}

		expectedResult := map[string]any{
			"read_value": "value_" + strconv.Itoa(requestID),
			"request_id": requestID,
			"status":     "ok",
		}
		log.Printf("Client %s sending request %d to %v", c.Name, requestID, c.Next)

		for _, nextNode := range c.Next {
			_, err := common.SendMessage(nextNode, 8000, request)
			if err != nil {
				log.Printf("Failed to send to %s: %v", nextNode, err)
				logger.LogRequest(requestID, nextNode, "error", request, expectedResult)
				continue
			}
			log.Printf("Ack from shim %s", nextNode)
			logger.LogRequest(requestID, nextNode, "ack", request, expectedResult)
		}
	}
}

func ExecuteRequest(e *nodes.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	requestID := request["request_id"]
	op, _ := request["op"].(string)
	opPayload, _ := request["op_payload"].(map[string]any)

	// Execute a single request and return the response
	response := map[string]any{"request_id": requestID}

	switch op {
	case "spin_write_read":
		spinTime := getFloat(opPayload, "spin_time")
		writeKey := getString(opPayload, "write_key")
		writeValue := getString(opPayload, "write_value")
		readKey := getString(opPayload, "read_key")

		// Spin for the given time
		if spinTime > 0 {
			time.Sleep(time.Duration(spinTime * float64(time.Second)))
		}

		// Write to key
		e.WriteKV(writeKey, writeValue)
		// Read from key
		response["read_value"] = e.ReadKV(readKey)
		response["status"] = "ok"
	default:
		response["status"] = "error"
		response["error"] = "Unknown op: " + op
	}

	_ = ndSeed
	_ = ndTimestamp
	return response
}

func ExecuteRequestFanout(e *nodes.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	response := ExecuteRequest(e, request, ndSeed, ndTimestamp)

	fanoutTargets := []string{"node7", "node8", "node9"}
	var fanoutResponse map[string]any
	for _, target := range fanoutTargets {
		outgoing := map[string]any{
			"type":       "request",
			"request_id": request["request_id"],
			"timestamp":  request["timestamp"],
			"sender":     e.Name,
			"op":         request["op"],
			"op_payload": request["op_payload"],
		}
		resp, err := common.SendMessage(target, 8000, outgoing)
		if err != nil {
			log.Printf("Fanout from %s to %s failed: %v", e.Name, target, err)
			continue
		}
		if fanoutResponse == nil {
			fanoutResponse = resp
		}
	}

	if fanoutResponse != nil {
		return fanoutResponse
	}
	return response
}

func ResponseForwardToClients(e *nodes.Exec, payload map[string]any) map[string]any {
	sender, _ := payload["sender"].(string)
	if !responseQuorum.Add(payload["request_id"], sender) {
		return map[string]any{"status": "waiting_for_quorum", "request_id": payload["request_id"]}
	}
	clientResponse := map[string]any{
		"type":       "response",
		"request_id": payload["request_id"],
		"response":   payload["response"],
		"sender":     sender,
	}

	clients := []string{"node1", "node2", "node3"}
	for _, client := range clients {
		if _, err := common.SendMessage(client, 8000, clientResponse); err != nil {
			log.Printf("Failed to send response to %s: %v", client, err)
		}
	}
	return map[string]any{"status": "response_broadcast", "recipients": clients}
}

func ResponseNoop(_ *nodes.Exec, payload map[string]any) map[string]any {
	return map[string]any{"status": "ignored_response", "request_id": payload["request_id"]}
}

func getString(m map[string]any, key string) string {
	if value, ok := m[key]; ok {
		if s, ok := value.(string); ok {
			return s
		}
	}
	return ""
}

func getFloat(m map[string]any, key string) float64 {
	if value, ok := m[key]; ok {
		switch v := value.(type) {
		case float64:
			return v
		case int:
			return float64(v)
		case int64:
			return float64(v)
		}
	}
	return 0
}
