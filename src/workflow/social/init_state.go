package socialworkflow

import (
	"aegean/common"
	"aegean/components/exec"
	"fmt"
)

func InitState(e *exec.Exec) map[string]string {
	serviceName := common.MustString(e.RunConfig, "service_name")
	switch serviceName {
	case "social_graph":
		return initSocialGraphState(e)
	default:
		return map[string]string{}
	}
}

func initSocialGraphState(e *exec.Exec) map[string]string {
	userCount := common.MustInt(e.RunConfig, "social_user_count")
	followersPerUser := common.IntOrDefault(e.RunConfig, "social_followers_per_user", 3)
	if userCount <= 0 {
		return map[string]string{}
	}
	if followersPerUser < 0 {
		followersPerUser = 0
	}
	if followersPerUser >= userCount {
		followersPerUser = userCount - 1
	}

	state := make(map[string]string, userCount)
	for userIdx := 0; userIdx < userCount; userIdx++ {
		userID := socialUserID(userIdx)
		followers := make([]string, 0, followersPerUser)
		followees := make([]string, 0, followersPerUser)
		for offset := 1; offset <= followersPerUser; offset++ {
			followerIdx := (userIdx - offset + userCount) % userCount
			followeeIdx := (userIdx + offset) % userCount
			followers = append(followers, socialUserID(followerIdx))
			followees = append(followees, socialUserID(followeeIdx))
		}
		entry := SocialGraphEntry{
			UserID:    userID,
			Followers: uniqueSortedStrings(followers),
			Followees: uniqueSortedStrings(followees),
		}
		state[socialGraphKey(userID)] = encodeSocialGraphEntry(entry)
	}
	return state
}

func socialUserID(userIdx int) string {
	return fmt.Sprintf("user-%d", userIdx)
}
