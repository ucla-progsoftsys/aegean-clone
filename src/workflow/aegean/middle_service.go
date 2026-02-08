package aegeanworkflow

import (
	"log"
	"time"

	"aegean/common"
	"aegean/components/exec"
)

func ExecuteRequest(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	requestID := request["request_id"]
	op, _ := request["op"].(string)
	opPayload, _ := request["op_payload"].(map[string]any)

	// Execute a single request and return the response
	response := map[string]any{"request_id": requestID}

	switch op {
	case "spin_write_read":
		spinTime := common.GetFloat(opPayload, "spin_time")
		writeKey := common.GetString(opPayload, "write_key")
		writeValue := common.GetString(opPayload, "write_value")
		readKey := common.GetString(opPayload, "read_key")

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

func ExecuteRequestFanout(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	// First stage: do local work and fan out nested requests asynchronously.
	if request["__fanout_waiting"] == nil {
		response := ExecuteRequest(e, request, ndSeed, ndTimestamp)
		request["__base_response"] = response
		request["__fanout_waiting"] = true

		fanoutTargets := []string{"node7", "node8", "node9"}
		for _, target := range fanoutTargets {
			outgoing := map[string]any{
				"type":       "request",
				"request_id": request["request_id"],
				"timestamp":  request["timestamp"],
				"sender":     e.Name,
				"op":         request["op"],
				"op_payload": request["op_payload"],
			}
			go func(target string, outgoing map[string]any) {
				_, err := common.SendMessage(target, 8000, outgoing)
				if err != nil {
					log.Printf("Fanout from %s to %s failed: %v", e.Name, target, err)
				}
			}(target, outgoing)
		}

		return map[string]any{
			"status":     "blocked_for_nested_response",
			"request_id": request["request_id"],
		}
	}

	// Continuation stage: once scheduler attaches nested response, return it
	if nested, ok := request["__nested_response"].(map[string]any); ok && nested != nil {
		delete(request, "__nested_response")
		if fanoutDone, output := processNestedFanoutResponse(request, nested); fanoutDone {
			return output
		}
		return map[string]any{
			"status":     "blocked_for_nested_response",
			"request_id": request["request_id"],
		}
	}
	return map[string]any{
		"status":     "blocked_for_nested_response",
		"request_id": request["request_id"],
	}
}

func processNestedFanoutResponse(request map[string]any, nested map[string]any) (bool, map[string]any) {
	if shimAggregated, _ := nested["shim_quorum_aggregated"].(bool); !shimAggregated {
		return false, nil
	}

	output := map[string]any{
		"request_id": request["request_id"],
		"status":     "ok",
	}
	if selectedResponse, ok := nested["response"].(map[string]any); ok {
		output["response"] = selectedResponse
	}
	if base, ok := request["__base_response"].(map[string]any); ok && base != nil {
		for key, value := range base {
			if _, exists := output[key]; !exists {
				output[key] = value
			}
		}
	}
	return true, output
}
