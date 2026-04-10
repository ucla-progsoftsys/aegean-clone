package socialworkflow

import "aegean/components/exec"

// SocialGraph is the follower index. In this subset it is primarily used by
// home_timeline to discover which users should receive a fanout write.
func ExecuteRequestSocialGraph(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed
	_ = ndTimestamp

	requestID := request["request_id"]
	op, _ := request["op"].(string)

	switch op {
	case "insert_user":
		// insert_user: initialize one user's follower/followee entry locally.
		userID := commonPayloadString(request, "user_id")
		if userID == "" {
			return errorResponse(requestID, "missing user_id")
		}
		entry := SocialGraphEntry{
			UserID:    userID,
			Followers: []string{},
			Followees: []string{},
		}
		socialWriteKV(e, socialGraphKey(userID), encodeSocialGraphEntry(entry))
		return nestedOkResponse(request)
	case "follow":
		// follow: update both endpoints locally by adding the followee to the
		// follower's followees and the follower to the followee's followers.
		followerID := commonPayloadString(request, "follower_id")
		followeeID := commonPayloadString(request, "followee_id")
		if followerID == "" || followeeID == "" {
			return errorResponse(requestID, "missing follower or followee")
		}
		followerEntry, _ := decodeSocialGraphEntry(socialReadKV(e, socialGraphKey(followerID)))
		followerEntry.UserID = followerID
		followerEntry.Followees = uniqueSortedStrings(append(followerEntry.Followees, followeeID))
		socialWriteKV(e, socialGraphKey(followerID), encodeSocialGraphEntry(followerEntry))

		followeeEntry, _ := decodeSocialGraphEntry(socialReadKV(e, socialGraphKey(followeeID)))
		followeeEntry.UserID = followeeID
		followeeEntry.Followers = uniqueSortedStrings(append(followeeEntry.Followers, followerID))
		socialWriteKV(e, socialGraphKey(followeeID), encodeSocialGraphEntry(followeeEntry))
		return nestedOkResponse(request)
	case "get_followers", "ro_get_followers":
		// get_followers: read one user's follower list locally. home_timeline uses
		// this to decide which home_timeline keys to update during fanout.
		// write_home_timeline uses this read path before updating follower feeds.
		userID := commonPayloadString(request, "user_id")
		entry, _ := decodeSocialGraphEntry(socialReadKV(e, socialGraphKey(userID)))
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
		// get_followees: read one user's followee list locally.
		userID := commonPayloadString(request, "user_id")
		entry, _ := decodeSocialGraphEntry(socialReadKV(e, socialGraphKey(userID)))
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
