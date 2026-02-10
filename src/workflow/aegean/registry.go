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
	ExecWorkflows["backend"] = ExecuteRequest
	ExecWorkflows["middle"] = ExecuteRequestFanout
}
