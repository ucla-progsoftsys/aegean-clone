package aegeanworkflow

import (
	"aegean/components/exec"
	"aegean/nodes"
)

var ClientWorkflows = map[string]func(c *nodes.Client){}
var ExecWorkflows = map[string]exec.ExecuteRequestFunc{}

func init() {
	ClientWorkflows["default"] = ClientRequestLogicWaitForResponse
	ClientWorkflows["pipelined"] = ClientRequestLogic
	ExecWorkflows["backend"] = ExecuteRequestBackend
	ExecWorkflows["backend_diverge_1"] = ExecuteRequestBackendDivergeOneNode
	ExecWorkflows["backend_diverge_2"] = ExecuteRequestBackendDivergeTwoNode
	ExecWorkflows["backend_diverge_3"] = ExecuteRequestBackendDivergeThreeNode
	ExecWorkflows["middle"] = ExecRequestMiddle
	ExecWorkflows["middle_diverge_1"] = ExecRequestMiddleDivergeOneNode
	ExecWorkflows["middle_diverge_2"] = ExecRequestMiddleDivergeTwoNode
	ExecWorkflows["middle_diverge_3"] = ExecRequestMiddleDivergeThreeNode
}
