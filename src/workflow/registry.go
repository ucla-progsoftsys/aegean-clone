package workflow

import (
	"aegean/components/exec"
	"aegean/nodes"
	aegeanworkflow "aegean/workflow/aegean"
	externalsrvworkflow "aegean/workflow/external_srv"
	hotelworkflow "aegean/workflow/hotel"
	mediaworkflow "aegean/workflow/media"
	reqraceworkflow "aegean/workflow/req_race"
	socialworkflow "aegean/workflow/social"
	supersimpleworkflow "aegean/workflow/supersimple"
)

var ClientWorkflows = map[string]func(c *nodes.Client){
	"aegean_oha_client":                          aegeanworkflow.OhaClientRequestLogic,
	"aegean_k6_client":                           aegeanworkflow.K6OpenClientRequestLogic,
	"aegean_k6_open_client":                      aegeanworkflow.K6OpenClientRequestLogic,
	"aegean_k6_closed_client":                    aegeanworkflow.K6ClosedClientRequestLogic,
	"external_srv_oha_client":                    externalsrvworkflow.OhaClientRequestLogic,
	"external_srv_k6_closed_client":              externalsrvworkflow.K6ClosedClientRequestLogic,
	"hotel_k6_closed_client":                     hotelworkflow.K6ClosedClientRequestLogic,
	"hotel_k6_closed_hotels_client":              hotelworkflow.K6ClosedHotelsClientRequestLogic,
	"hotel_k6_closed_recommendations_client":     hotelworkflow.K6ClosedRecommendationsClientRequestLogic,
	"hotel_k6_open_hotels_client":                hotelworkflow.K6OpenHotelsClientRequestLogic,
	"hotel_k6_open_recommendations_client":       hotelworkflow.K6OpenRecommendationsClientRequestLogic,
	"hotel_k6_open_reservation_client":           hotelworkflow.K6OpenReservationClientRequestLogic,
	"media_k6_closed_client":                     mediaworkflow.K6ClosedReviewComposeClientRequestLogic,
	"media_k6_closed_review_compose_client":      mediaworkflow.K6ClosedReviewComposeClientRequestLogic,
	"media_k6_open_review_compose_client":        mediaworkflow.K6OpenReviewComposeClientRequestLogic,
	"req_race_oha_client":                        reqraceworkflow.OhaClientRequestLogic,
	"req_race_k6_closed_client":                  reqraceworkflow.K6ClosedClientRequestLogic,
	"social_k6_closed_client":                    socialworkflow.K6ClosedClientRequestLogic,
	"social_k6_closed_read_home_timeline_client": socialworkflow.K6ClosedReadHomeTimelineClientRequestLogic,
	"social_k6_closed_read_user_timeline_client": socialworkflow.K6ClosedReadUserTimelineClientRequestLogic,
	"social_k6_open_client":                      socialworkflow.K6OpenClientRequestLogic,
	"social_k6_open_read_home_timeline_client":   socialworkflow.K6OpenReadHomeTimelineClientRequestLogic,
	"social_k6_open_read_user_timeline_client":   socialworkflow.K6OpenReadUserTimelineClientRequestLogic,
	"supersimple_oha_client":                     supersimpleworkflow.OhaClientRequestLogic,
	"supersimple_k6_closed_client":               supersimpleworkflow.K6ClosedClientRequestLogic,
}

var ExecWorkflows = map[string]exec.ExecuteRequestFunc{
	"aegean_backend":           aegeanworkflow.ExecuteRequestBackend,
	"aegean_backend_diverge_1": aegeanworkflow.ExecuteRequestBackendDivergeOneNode,
	"aegean_backend_diverge_2": aegeanworkflow.ExecuteRequestBackendDivergeTwoNode,
	"aegean_backend_diverge_3": aegeanworkflow.ExecuteRequestBackendDivergeThreeNode,
	"aegean_middle":            aegeanworkflow.ExecuteRequestMiddle,
	"aegean_middle_diverge_1":  aegeanworkflow.ExecuteRequestMiddleDivergeOneNode,
	"aegean_middle_diverge_2":  aegeanworkflow.ExecuteRequestMiddleDivergeTwoNode,
	"aegean_middle_diverge_3":  aegeanworkflow.ExecuteRequestMiddleDivergeThreeNode,
	"external_srv_server":      externalsrvworkflow.ExecuteRequestServer,
	"hotel_frontend":           hotelworkflow.ExecuteRequestFrontend,
	"hotel_search":             hotelworkflow.ExecuteRequestSearch,
	"hotel_geo":                hotelworkflow.ExecuteRequestGeo,
	"hotel_rate":               hotelworkflow.ExecuteRequestRate,
	"hotel_profile":            hotelworkflow.ExecuteRequestProfile,
	"hotel_recommendation":     hotelworkflow.ExecuteRequestRecommendation,
	"hotel_user":               hotelworkflow.ExecuteRequestUser,
	"hotel_reservation":        hotelworkflow.ExecuteRequestReservation,
	"media_review_compose_api": mediaworkflow.ExecuteRequestReviewComposeAPI,
	"media_user":               mediaworkflow.ExecuteRequestUser,
	"media_movie_id":           mediaworkflow.ExecuteRequestMovieID,
	"media_text":               mediaworkflow.ExecuteRequestText,
	"media_unique_id":          mediaworkflow.ExecuteRequestUniqueID,
	"media_rating":             mediaworkflow.ExecuteRequestRating,
	"media_compose_review":     mediaworkflow.ExecuteRequestComposeReview,
	"media_review_storage":     mediaworkflow.ExecuteRequestReviewStorage,
	"media_user_review":        mediaworkflow.ExecuteRequestUserReview,
	"media_movie_review":       mediaworkflow.ExecuteRequestMovieReview,
	"req_race_middle":          reqraceworkflow.ExecuteRequestMiddle,
	"req_race_backend_1":       reqraceworkflow.ExecuteRequestBackend1,
	"req_race_backend_2":       reqraceworkflow.ExecuteRequestBackend2,
	"req_race_backend_3":       reqraceworkflow.ExecuteRequestBackend3,
	"social_compose_post":      socialworkflow.ExecuteRequestComposePost,
	"social_post_storage":      socialworkflow.ExecuteRequestPostStorage,
	"social_user_timeline":     socialworkflow.ExecuteRequestUserTimeline,
	"social_home_timeline":     socialworkflow.ExecuteRequestHomeTimeline,
	"social_social_graph":      socialworkflow.ExecuteRequestSocialGraph,
	"supersimple_middle":       supersimpleworkflow.ExecuteRequestMiddle,
	"supersimple_backend":      supersimpleworkflow.ExecuteRequestServer,
}

var InitStateWorkflows = map[string]exec.InitStateFunc{
	"default":              aegeanworkflow.InitState,
	"aegean_default":       aegeanworkflow.InitState,
	"external_srv_default": externalsrvworkflow.InitState,
	"hotel_default":        hotelworkflow.InitState,
	"media_default":        mediaworkflow.InitState,
	"req_race_default":     reqraceworkflow.InitState,
	"social_default":       socialworkflow.InitState,
	"supersimple_default":  supersimpleworkflow.InitState,
}

var ExternalServiceInitWorkflows = map[string]func(es *nodes.ExternalService){
	"default":              externalsrvworkflow.InitExternalService,
	"external_srv_default": externalsrvworkflow.InitExternalService,
}

var ExternalServiceWorkflows = map[string]func(es *nodes.ExternalService, payload map[string]any) map[string]any{
	"external_srv_external_service": externalsrvworkflow.ExternalServiceLogic,
}
