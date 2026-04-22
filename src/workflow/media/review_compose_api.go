package mediaworkflow

import "aegean/components/exec"

const (
	reviewComposeAPIStageContextKey = "media_review_compose_api_stage"
	reviewComposeAPIStageAwait      = "await_first_tier"
)

var (
	mediaUserTargets     = []string{"node4", "node5", "node6"}
	mediaMovieIDTargets  = []string{"node7", "node8", "node9"}
	mediaTextTargets     = []string{"node10", "node11", "node12"}
	mediaUniqueIDTargets = []string{"node13", "node14", "node15"}
)

func ExecuteRequestReviewComposeAPI(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed

	requestID := request["request_id"]
	op, _ := request["op"].(string)
	if op != "compose_review" {
		return mediaErrorResponse(requestID, "unsupported op: "+op)
	}

	stageAny, _ := e.GetRequestContextValue(requestID, reviewComposeAPIStageContextKey)
	stage, _ := stageAny.(string)
	switch stage {
	case "":
		payload := mediaRequestPayload(request)
		if validationErr := validateReviewComposePayload(requestID, payload); validationErr != nil {
			return validationErr
		}
		if !e.SetRequestContextValue(requestID, reviewComposeAPIStageContextKey, reviewComposeAPIStageAwait) {
			return mediaErrorResponse(requestID, "failed to initialize review compose api context")
		}
		dispatchReviewComposeFanout(e, request, payload, ndTimestamp)
		return mediaBlockedForNestedResponse(requestID)
	case reviewComposeAPIStageAwait:
		nestedResponses, ok := e.GetNestedResponses(requestID)
		if !ok || !mediaNestedResponsesReady(nestedResponses, requestID, "user", "movie_id", "text", "unique_id") {
			return mediaBlockedForNestedResponse(requestID)
		}
		response := map[string]any{
			"request_id": requestID,
			"status":     "ok",
		}
		if uniqueNested := mediaSelectedNestedResponse(nestedResponses, requestID, "unique_id"); uniqueNested != nil {
			uniquePayload := mediaNestedResponsePayload(uniqueNested)
			if reviewID, ok := mediaPayloadInt64(uniquePayload, "review_id"); ok {
				response["review_id"] = reviewID
			}
		}
		e.ClearRequestContext(requestID)
		return response
	default:
		return mediaErrorResponse(requestID, "unknown review compose api stage: "+stage)
	}
}

func validateReviewComposePayload(requestID any, payload map[string]any) map[string]any {
	for _, key := range []string{"username", "password", "title", "text"} {
		if mediaPayloadString(payload, key) == "" {
			return mediaErrorResponse(requestID, "missing "+key)
		}
	}
	if _, ok := mediaPayloadInt(payload, "rating"); !ok {
		return mediaErrorResponse(requestID, "missing rating")
	}
	return nil
}

func dispatchReviewComposeFanout(e *exec.Exec, request map[string]any, payload map[string]any, ndTimestamp float64) {
	requestID := request["request_id"]
	reviewRequestID := mediaReviewRequestIDFromPayload(payload, requestID)
	rating, _ := mediaPayloadInt(payload, "rating")

	userRequest := mediaNewNestedRequest(requestID, "user", ndTimestamp, "upload_user_with_username", map[string]any{
		"review_request_id": reviewRequestID,
		"username":          mediaPayloadString(payload, "username"),
		"password":          mediaPayloadString(payload, "password"),
	})
	mediaDispatchNestedRequest(e, request, mediaUserTargets, userRequest)

	movieIDRequest := mediaNewNestedRequest(requestID, "movie_id", ndTimestamp, "upload_movie_id", map[string]any{
		"review_request_id": reviewRequestID,
		"title":             mediaPayloadString(payload, "title"),
		"rating":            rating,
	})
	mediaDispatchNestedRequest(e, request, mediaMovieIDTargets, movieIDRequest)

	textRequest := mediaNewNestedRequest(requestID, "text", ndTimestamp, "upload_text", map[string]any{
		"review_request_id": reviewRequestID,
		"text":              mediaPayloadString(payload, "text"),
	})
	mediaDispatchNestedRequest(e, request, mediaTextTargets, textRequest)

	uniqueIDRequest := mediaNewNestedRequest(requestID, "unique_id", ndTimestamp, "upload_unique_id", map[string]any{
		"review_request_id": reviewRequestID,
	})
	mediaDispatchNestedRequest(e, request, mediaUniqueIDTargets, uniqueIDRequest)
}
