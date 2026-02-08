package shim

import (
	"log"

	"aegean/common"
)

type Shim struct {
	Name                 string
	BatcherCh            chan<- map[string]any
	ExecCh               chan<- map[string]any
	Clients              []string
	requestQuorumHelper  *common.QuorumHelper
	responseQuorumHelper *common.QuorumHelper
}

func NewShim(name string, batcherCh chan<- map[string]any, execCh chan<- map[string]any, clients []string) *Shim {
	if batcherCh == nil {
		log.Fatalf("shim component requires non-nil batcherCh")
	}
	shim := &Shim{
		Name:                 name,
		BatcherCh:            batcherCh,
		ExecCh:               execCh,
		Clients:              clients,
		requestQuorumHelper:  common.NewQuorumHelper(2), // TODO: Replace hard-coded value with formula
		responseQuorumHelper: common.NewQuorumHelper(2), // TODO: Replace hard-coded value with formula
	}
	return shim
}

func (s *Shim) HandleRequestMessage(payload map[string]any) map[string]any {
	log.Printf("Handler called on %s with payload: %v", s.Name, payload)

	msgType, _ := payload["type"].(string)
	if msgType == "" {
		msgType = "request"
	}

	// Handle incoming client request - wait for quorum then forward
	requestID := payload["request_id"]
	sender, _ := payload["sender"].(string)

	if s.requestQuorumHelper.Add(requestID, sender) {
		if s.BatcherCh != nil {
			s.BatcherCh <- payload
		} else {
			log.Printf("%s: Batcher channel not set; dropping request %v", s.Name, requestID)
		}
		return map[string]any{"status": "forwarded_to_mid_execs"}
	}
	return map[string]any{"status": "waiting_for_quorum"}
}

func (s *Shim) HandleIncomingResponse(payload map[string]any) map[string]any {
	log.Printf("Handler called on %s with payload: %v", s.Name, payload)

	requestID := payload["request_id"]
	sender, _ := payload["sender"].(string)
	if sender == "" {
		return map[string]any{"status": "error", "error": "missing sender"}
	}

	if !s.responseQuorumHelper.Add(requestID, sender) {
		return map[string]any{"status": "waiting_for_quorum", "request_id": requestID}
	}

	// This assumes nested responses are equivalent across backends
	payload["shim_quorum_aggregated"] = true

	if s.ExecCh != nil {
		s.ExecCh <- payload
		return map[string]any{"status": "forwarded_nested_response", "request_id": requestID}
	}
	return map[string]any{"status": "error", "error": "exec channel not configured", "request_id": requestID}
}

func (s *Shim) HandleOutgoingResponse(payload map[string]any) map[string]any {
	requestID := payload["request_id"]
	responseData, _ := payload["response"].(map[string]any)
	sender := s.Name

	// Handle response from exec - broadcast to all clients that sent the request
	// TODO: Or do we wait for a quorum, and then broadcast
	log.Printf("%s: Broadcasting response for request %v to %d clients: %v", s.Name, requestID, len(s.Clients), s.Clients)

	for _, client := range s.Clients {
		clientResponse := map[string]any{
			"type":       "response",
			"request_id": requestID,
			"response":   responseData,
			"sender":     sender,
		}
		if _, err := common.SendMessage(client, 8000, clientResponse); err != nil {
			log.Printf("%s: Failed to send response to %s: %v", s.Name, client, err)
			continue
		}
		log.Printf("%s: Sent response to client %s", s.Name, client)
	}

	return map[string]any{"status": "response_broadcast", "recipients": s.Clients}
}
