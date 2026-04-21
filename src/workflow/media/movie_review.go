package mediaworkflow

import (
	"aegean/common"
	"aegean/components/exec"
)

func ExecuteRequestMovieReview(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed
	_ = ndTimestamp

	requestID := request["request_id"]
	op, _ := request["op"].(string)
	switch op {
	case "upload_movie_review":
		payload := mediaRequestPayload(request)
		movieID := mediaPayloadString(payload, "movie_id")
		reviewID, okReviewID := mediaPayloadInt64(payload, "review_id")
		timestamp, okTimestamp := mediaPayloadInt64(payload, "timestamp")
		if movieID == "" || !okReviewID || !okTimestamp {
			return mediaErrorResponse(requestID, "missing movie review payload")
		}
		key := mediaMovieReviewKey(movieID)
		existing := decodeMediaReviewIndex(mediaReadKV(e, key))
		maxEntries := common.IntOrDefault(e.RunConfig, "media_max_reviews_per_index", 100)
		updated := prependMediaReviewIndex(existing, MediaReviewIndexEntry{ReviewID: reviewID, Timestamp: timestamp}, maxEntries)
		mediaWriteKV(e, key, encodeMediaReviewIndex(updated))
		return mediaNestedOkResponse(request)
	default:
		return mediaErrorResponse(requestID, "unsupported op: "+op)
	}
}
