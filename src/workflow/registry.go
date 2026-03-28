package workflow

import (
	"aegean/components/exec"
	"aegean/nodes"
	aegeanworkflow "aegean/workflow/aegean"
	externalsrvworkflow "aegean/workflow/external_srv"
	reqraceworkflow "aegean/workflow/req_race"
	socialworkflow "aegean/workflow/social"
	supersimpleworkflow "aegean/workflow/supersimple"
)

var ClientWorkflows = map[string]func(c *nodes.Client){
	"aegean_oha_client":             aegeanworkflow.OhaClientRequestLogic,
	"aegean_k6_client":              aegeanworkflow.K6OpenClientRequestLogic,
	"aegean_k6_open_client":         aegeanworkflow.K6OpenClientRequestLogic,
	"aegean_k6_closed_client":       aegeanworkflow.K6ClosedClientRequestLogic,
	"external_srv_oha_client":       externalsrvworkflow.OhaClientRequestLogic,
	"external_srv_k6_closed_client": externalsrvworkflow.K6ClosedClientRequestLogic,
	"req_race_oha_client":           reqraceworkflow.OhaClientRequestLogic,
	"req_race_k6_closed_client":     reqraceworkflow.K6ClosedClientRequestLogic,
	"social_k6_closed_client":       socialworkflow.K6ClosedClientRequestLogic,
	"supersimple_oha_client":        supersimpleworkflow.OhaClientRequestLogic,
	"supersimple_k6_closed_client":  supersimpleworkflow.K6ClosedClientRequestLogic,
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
