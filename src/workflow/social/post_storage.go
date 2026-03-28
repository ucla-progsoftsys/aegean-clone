package socialworkflow

import "aegean/components/exec"

func ExecuteRequestPostStorage(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed
	_ = ndTimestamp

	requestID := request["request_id"]
	op, _ := request["op"].(string)
	opPayload, _ := request["op_payload"].(map[string]any)

	switch op {
	case "store_post":
		postID, _ := opPayload["post_id"].(string)
		creatorID, _ := opPayload["creator_id"].(string)
		text, _ := opPayload["text"].(string)
		timestamp := int64(0)
		switch typed := opPayload["timestamp"].(type) {
		case int64:
			timestamp = typed
		case int:
			timestamp = int64(typed)
		case float64:
			timestamp = int64(typed)
		}
		if postID == "" || creatorID == "" {
			return errorResponse(requestID, "missing post payload")
		}
		post := Post{
			PostID:    postID,
			Timestamp: timestamp,
			Text:      text,
			CreatorID: creatorID,
		}
		e.WriteKV(postKey(postID), encodePost(post))
		return nestedOkResponseWithPostID(request, postID)
	case "read_post", "ro_read_post":
		postID, _ := opPayload["post_id"].(string)
		post, ok := decodePost(e.ReadKV(postKey(postID)))
		if !ok {
			return errorResponse(requestID, "post not found")
		}
		return map[string]any{
			"request_id": requestID,
			"status":     "ok",
			"post": map[string]any{
				"post_id":    post.PostID,
				"timestamp":  post.Timestamp,
				"text":       post.Text,
				"creator_id": post.CreatorID,
			},
		}
	case "read_posts", "ro_read_posts":
		postIDs := commonPayloadStringSlice(request, "post_ids")
		posts := make([]map[string]any, 0, len(postIDs))
		for _, postID := range postIDs {
			post, ok := decodePost(e.ReadKV(postKey(postID)))
			if !ok {
				continue
			}
			posts = append(posts, map[string]any{
				"post_id":    post.PostID,
				"timestamp":  post.Timestamp,
				"text":       post.Text,
				"creator_id": post.CreatorID,
			})
		}
		return map[string]any{
			"request_id": requestID,
			"status":     "ok",
			"posts":      posts,
		}
	default:
		return errorResponse(requestID, "unsupported op: "+op)
	}
}
