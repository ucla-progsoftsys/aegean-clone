package socialworkflow

import "aegean/components/exec"

func ExecuteRequestSocialGraph(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed
	_ = ndTimestamp

	requestID := request["request_id"]
	op, _ := request["op"].(string)

	switch op {
	case "insert_user":
		userID := commonPayloadString(request, "user_id")
		if userID == "" {
			return errorResponse(requestID, "missing user_id")
		}
		entry := SocialGraphEntry{
			UserID:    userID,
			Followers: []string{},
			Followees: []string{},
		}
		e.WriteKV(socialGraphKey(userID), encodeSocialGraphEntry(entry))
		return nestedOkResponse(request)
	case "follow":
		followerID := commonPayloadString(request, "follower_id")
		followeeID := commonPayloadString(request, "followee_id")
		if followerID == "" || followeeID == "" {
			return errorResponse(requestID, "missing follower or followee")
		}
		followerEntry, _ := decodeSocialGraphEntry(e.ReadKV(socialGraphKey(followerID)))
		followerEntry.UserID = followerID
		followerEntry.Followees = uniqueSortedStrings(append(followerEntry.Followees, followeeID))
		e.WriteKV(socialGraphKey(followerID), encodeSocialGraphEntry(followerEntry))

		followeeEntry, _ := decodeSocialGraphEntry(e.ReadKV(socialGraphKey(followeeID)))
		followeeEntry.UserID = followeeID
		followeeEntry.Followers = uniqueSortedStrings(append(followeeEntry.Followers, followerID))
		e.WriteKV(socialGraphKey(followeeID), encodeSocialGraphEntry(followeeEntry))
		return nestedOkResponse(request)
	case "get_followers", "ro_get_followers":
		userID := commonPayloadString(request, "user_id")
		entry, _ := decodeSocialGraphEntry(e.ReadKV(socialGraphKey(userID)))
		response := map[string]any{
			"request_id": requestID,
			"status":     "ok",
			"followers":  entry.Followers,
		}
		if parentRequestID, ok := request["parent_request_id"]; ok && parentRequestID != nil {
			response["parent_request_id"] = parentRequestID
		}
		return response
	case "get_followees", "ro_get_followees":
		userID := commonPayloadString(request, "user_id")
		entry, _ := decodeSocialGraphEntry(e.ReadKV(socialGraphKey(userID)))
		response := map[string]any{
			"request_id": requestID,
			"status":     "ok",
			"followees":  entry.Followees,
		}
		if parentRequestID, ok := request["parent_request_id"]; ok && parentRequestID != nil {
			response["parent_request_id"] = parentRequestID
		}
		return response
	default:
		return errorResponse(requestID, "unsupported op: "+op)
	}
}
