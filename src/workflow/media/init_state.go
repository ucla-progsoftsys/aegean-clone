package mediaworkflow

import (
	"aegean/common"
	"aegean/components/exec"
	"strconv"
)

func InitState(e *exec.Exec) map[string]string {
	serviceName := common.MustString(e.RunConfig, "service_name")
	var state map[string]string
	switch serviceName {
	case "user":
		state = initMediaUserState(e)
	case "movie_id":
		state = initMediaMovieIDState(e)
	case "rating":
		state = initMediaRatingState(e)
	default:
		state = map[string]string{}
	}
	if mediaServiceHasPersistentState(serviceName) {
		mediaPersistStateSeed(e, state)
	}
	return state
}

func mediaServiceHasPersistentState(serviceName string) bool {
	switch serviceName {
	case "movie_id", "rating", "user":
		return true
	default:
		return false
	}
}

func initMediaUserState(e *exec.Exec) map[string]string {
	userCount := common.MustInt(e.RunConfig, "media_user_count")
	if userCount <= 0 {
		return map[string]string{}
	}
	state := make(map[string]string, userCount)
	for userIdx := 0; userIdx < userCount; userIdx++ {
		username := mediaSeedUsername(userIdx)
		userID := int64(100000 + userIdx)
		state[mediaUserLookupKey(username)] = mediaInt64String(userID)
	}
	return state
}

func initMediaMovieIDState(e *exec.Exec) map[string]string {
	movieCount := common.MustInt(e.RunConfig, "media_movie_count")
	if movieCount <= 0 {
		return map[string]string{}
	}
	state := make(map[string]string, movieCount)
	for movieIdx := 0; movieIdx < movieCount; movieIdx++ {
		state[mediaMovieLookupKey(mediaSeedMovieTitle(movieIdx))] = mediaSeedMovieID(movieIdx)
	}
	return state
}

func initMediaRatingState(e *exec.Exec) map[string]string {
	movieCount := common.MustInt(e.RunConfig, "media_movie_count")
	if movieCount <= 0 {
		return map[string]string{}
	}
	state := make(map[string]string, movieCount*2)
	for movieIdx := 0; movieIdx < movieCount; movieIdx++ {
		movieID := mediaSeedMovieID(movieIdx)
		state[mediaRatingSumKey(movieID)] = strconv.Itoa(0)
		state[mediaRatingCountKey(movieID)] = strconv.Itoa(0)
	}
	return state
}
