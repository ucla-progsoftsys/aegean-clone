package externalsrvworkflow

import (
	"aegean/common"
	"aegean/components/exec"
	netx "aegean/net"
	"fmt"
)

func ExecuteRequestServer(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed
	_ = ndTimestamp

	requestID := request["request_id"]
	stageAny, _ := e.GetRequestContextValue(requestID, "external_srv_stage")
	stage, _ := stageAny.(string)
	op, _ := request["op"].(string)
	if op != "external_call" {
		return map[string]any{
			"request_id": requestID,
			"status":     "error",
			"error":      "unsupported op: " + op,
		}
	}

	switch stage {
	case "":
		if !e.SetRequestContextValue(requestID, "external_srv_stage", "await_nested") {
			return map[string]any{
				"request_id": requestID,
				"status":     "error",
				"error":      "failed to initialize request continuation context",
			}
		}

		dispatchExternalServiceRequest(e, request)
		return map[string]any{
			"request_id": requestID,
			"status":     "blocked_for_nested_response",
		}
	case "await_nested":
		nestedResponses, hasNested := e.GetNestedResponses(requestID)
		if !hasNested {
			return map[string]any{
				"request_id": requestID,
				"status":     "blocked_for_nested_response",
			}
		}

		expectedNestedRequestID := fmt.Sprintf("%v/external_service", requestID)
		for _, nested := range nestedResponses {
			if nestedRequestID, _ := nested["request_id"].(string); nestedRequestID != expectedNestedRequestID {
				continue
			}
			if response, ok := nested["response"].(map[string]any); ok {
				final := map[string]any{
					"request_id": requestID,
				}
				for key, value := range response {
					if key == "request_id" {
						continue
					}
					final[key] = value
				}
				e.ClearRequestContext(requestID)
				return final
			}
		}

		return map[string]any{
			"request_id": requestID,
			"status":     "blocked_for_nested_response",
		}
	default:
		return map[string]any{
			"request_id": requestID,
			"status":     "error",
			"error":      "unknown external_srv stage: " + stage,
		}
	}
}

func InitState(e *exec.Exec) map[string]string {
	_ = e
	return map[string]string{}
}

func dispatchExternalServiceRequest(e *exec.Exec, request map[string]any) {
	requestID := request["request_id"]
	outgoing := map[string]any{
		"type":              "request",
		"request_id":        fmt.Sprintf("%v/external_service", requestID),
		"parent_request_id": requestID,
		"timestamp":         request["timestamp"],
		"op":                "external_call",
		"op_payload":        request["op_payload"],
	}
	prepared, ok := e.PrepareNestedRequestPayload(request, outgoing)
	if !ok {
		return
	}

	useEO := common.BoolOrDefault(e.RunConfig, "nested_use_eo", false)
	if useEO && !e.ClaimNestedRequestEO(prepared) {
		return
	}

	go func(requestID any, outgoing map[string]any) {
		externalResp, err := netx.SendMessage("node4", 8000, outgoing)
		if err != nil {
			response := map[string]any{
				"request_id":        outgoing["request_id"],
				"parent_request_id": requestID,
				"response": map[string]any{
					"request_id": requestID,
					"status":     "error",
					"error":      "external_service_call_failed",
				},
			}
			if useEO {
				response["shim_quorum_aggregated"] = true
				e.BufferExactOnceNestedResponse(response)
				return
			}
			e.BufferNestedResponse(response)
			return
		}

		response := map[string]any{
			"request_id":        outgoing["request_id"],
			"parent_request_id": requestID,
			"response":          externalResp,
		}
		if useEO {
			response["shim_quorum_aggregated"] = true
			e.BufferExactOnceNestedResponse(response)
			return
		}
		e.BufferNestedResponse(response)
	}(requestID, prepared)
}
