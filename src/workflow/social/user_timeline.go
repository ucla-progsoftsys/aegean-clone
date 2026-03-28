package socialworkflow

import "aegean/components/exec"

func ExecuteRequestUserTimeline(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed
	_ = ndTimestamp

	requestID := request["request_id"]
	op, _ := request["op"].(string)

	switch op {
	case "write_user_timeline":
		userID := commonPayloadString(request, "user_id")
		if userID == "" {
			return errorResponse(requestID, "missing user_id")
		}
		postIDs := commonPayloadStringSlice(request, "post_ids")
		existing := decodeStringSlice(e.ReadKV(userTimelineKey(userID)))
		e.WriteKV(userTimelineKey(userID), encodeStringSlice(appendTimelineEntries(existing, postIDs, 10)))
		return nestedOkResponse(request)
	case "read_user_timeline", "ro_read_user_timeline":
		userID := commonPayloadString(request, "user_id")
		postIDs := decodeStringSlice(e.ReadKV(userTimelineKey(userID)))
		return map[string]any{
			"request_id": requestID,
			"status":     "ok",
			"post_ids":   postIDs,
		}
	default:
		return errorResponse(requestID, "unsupported op: "+op)
	}
}
