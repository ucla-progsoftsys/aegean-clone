package mediaworkflow

import "aegean/components/exec"

const (
	mediaUniqueIDStageContextKey = "media_unique_id_stage"
	mediaUniqueIDStageAwait      = "await_compose_review"
)

func ExecuteRequestUniqueID(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	requestID := request["request_id"]
	op, _ := request["op"].(string)
	if op != "upload_unique_id" {
		return mediaErrorResponse(requestID, "unsupported op: "+op)
	}

	stageAny, _ := e.GetRequestContextValue(requestID, mediaUniqueIDStageContextKey)
	stage, _ := stageAny.(string)
	switch stage {
	case "":
		payload := mediaRequestPayload(request)
		reviewRequestID := mediaReviewRequestIDFromPayload(payload, requestID)
		reviewID := deterministicMediaReviewID(reviewRequestID, ndSeed, ndTimestamp)
		if !e.SetRequestContextValue(requestID, mediaUniqueIDStageContextKey, mediaUniqueIDStageAwait) {
			return mediaErrorResponse(requestID, "failed to initialize unique id context")
		}
		outgoing := mediaNewNestedRequest(requestID, "compose_review", ndTimestamp, "upload_unique_id", map[string]any{
			"review_request_id": reviewRequestID,
			"review_id":         reviewID,
		})
		mediaDispatchNestedRequest(e.Name, e.RunConfig, request, mediaComposeReviewTargets, outgoing)
		return mediaBlockedForNestedResponse(requestID)
	case mediaUniqueIDStageAwait:
		return mediaCompleteAfterComposeResponse(e, request)
	default:
		return mediaErrorResponse(requestID, "unknown unique id stage: "+stage)
	}
}
