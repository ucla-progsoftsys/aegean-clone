package socialworkflow

import (
	"aegean/components/exec"
	netx "aegean/net"
	"aegean/telemetry"
	"context"
)

const (
	homeTimelineStageContextKey   = "social_home_timeline_stage"
	homeTimelinePayloadContextKey = "social_home_timeline_payload"

	homeTimelineStageAwaitFollowers = "await_followers"
)

const socialGraphPrimaryNode = "node13"

func ExecuteRequestHomeTimeline(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed
	_ = ndTimestamp

	requestID := request["request_id"]
	op, _ := request["op"].(string)
	stageAny, _ := e.GetRequestContextValue(requestID, homeTimelineStageContextKey)
	stage, _ := stageAny.(string)

	switch op {
	case "write_home_timeline":
		switch stage {
		case "":
			userID := commonPayloadString(request, "user_id")
			if userID == "" {
				return errorResponse(requestID, "missing user_id")
			}
			postIDs := commonPayloadStringSlice(request, "post_ids")
			payload := map[string]any{
				"user_id":  userID,
				"post_ids": postIDs,
			}
			if !e.SetRequestContextValue(requestID, homeTimelinePayloadContextKey, payload) {
				return errorResponse(requestID, "failed to initialize home timeline context")
			}
			if !e.SetRequestContextValue(requestID, homeTimelineStageContextKey, homeTimelineStageAwaitFollowers) {
				return errorResponse(requestID, "failed to set home timeline stage")
			}

			outgoing := map[string]any{
				"type":              "request",
				"request_id":        nestedRequestID(requestID, "social_graph"),
				"parent_request_id": requestID,
				"timestamp":         request["timestamp"],
				"sender":            e.Name,
				"op":                "get_followers",
				"op_payload": map[string]any{
					"user_id": userID,
				},
			}
			ctx := telemetry.ExtractContext(context.Background(), request)
			telemetry.InjectContext(ctx, outgoing)
			go func() {
				_, _ = netx.SendMessage(socialGraphPrimaryNode, 8000, outgoing)
			}()
			return blockedForNestedResponse(requestID)

		case homeTimelineStageAwaitFollowers:
			nestedResponses, ok := e.GetNestedResponses(requestID)
			if !ok || len(nestedResponses) == 0 {
				return blockedForNestedResponse(requestID)
			}
			var selected map[string]any
			for _, nested := range nestedResponses {
				if shimAggregated, _ := nested["shim_quorum_aggregated"].(bool); !shimAggregated {
					continue
				}
				childRequestID, _ := nested["request_id"].(string)
				if childRequestID == nestedRequestID(requestID, "social_graph") {
					selected = nested
					break
				}
			}
			if selected == nil {
				return blockedForNestedResponse(requestID)
			}

			payloadAny, ok := e.GetRequestContextValue(requestID, homeTimelinePayloadContextKey)
			if !ok {
				return errorResponse(requestID, "missing home timeline payload")
			}
			payload, _ := payloadAny.(map[string]any)
			userID, _ := payload["user_id"].(string)
			postIDs := commonPayloadStringSlice(map[string]any{"op_payload": payload}, "post_ids")

			nestedResponse, _ := selected["response"].(map[string]any)
			followers := extractStringSlice(nestedResponse["followers"])
			for _, follower := range followers {
				existing := decodeStringSlice(e.ReadKV(homeTimelineKey(follower)))
				e.WriteKV(homeTimelineKey(follower), encodeStringSlice(appendTimelineEntries(existing, postIDs, 10)))
			}
			e.ClearRequestContext(requestID)
			response := map[string]any{
				"request_id": requestID,
				"status":     "ok",
				"user_id":    userID,
				"followers":  followers,
			}
			if parentRequestID, ok := request["parent_request_id"]; ok && parentRequestID != nil {
				response["parent_request_id"] = parentRequestID
			}
			return response

		default:
			return errorResponse(requestID, "unknown home timeline stage: "+stage)
		}
	case "read_home_timeline", "ro_read_home_timeline":
		userID := commonPayloadString(request, "user_id")
		postIDs := decodeStringSlice(e.ReadKV(homeTimelineKey(userID)))
		return map[string]any{
			"request_id": requestID,
			"status":     "ok",
			"post_ids":   postIDs,
		}
	default:
		return errorResponse(requestID, "unsupported op: "+op)
	}
}
