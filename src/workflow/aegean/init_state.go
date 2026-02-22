package aegeanworkflow

import (
	"strconv"
	"strings"

	"aegean/components/exec"
)

func InitState(e *exec.Exec) map[string]string {
	_ = e
	const (
		keyCount    = 1000
		valueLength = 10000
	)
	initial := make(map[string]string, keyCount)
	value := strings.Repeat("x", valueLength)
	for i := 1; i <= keyCount; i++ {
		initial[strconv.Itoa(i)] = value
	}
	return initial
}
