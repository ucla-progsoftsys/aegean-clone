package aegeanworkflow

import (
	"aegean/nodes"
	"fmt"
	"log"
	"os"
	"os/exec"
)

const (
	ohaTargetURL = "http://node2:8000/"
	ohaRequestN  = 1000
)

func OhaClientRequestLogic(c *nodes.Client) {
	_ = c

	command := fmt.Sprintf(
		`oha -n %d -m POST -H "Content-Type: application/json" -d '{"oha_client":true}' %s`,
		ohaRequestN,
		ohaTargetURL,
	)
	cmd := exec.Command(
		"bash",
		"-lc",
		command,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		log.Printf("oha client request logic failed: %v", err)
	}
}
