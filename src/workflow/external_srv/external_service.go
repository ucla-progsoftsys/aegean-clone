package externalsrvworkflow

import (
	"aegean/common"
	"aegean/nodes"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

var (
	idempotentMu          sync.Mutex
	idempotentByRequestID = map[string]float64{}
	sideEffectCallCount   atomic.Uint64
	monotonicMu           sync.RWMutex
	monotonicValue        float64
	monotonicCancel       func()
)

func InitExternalService(es *nodes.ExternalService) {
	_ = es
	idempotentMu.Lock()
	idempotentByRequestID = map[string]float64{}
	idempotentMu.Unlock()
	sideEffectCallCount.Store(0)
	resetMonotonicUpdater()
}

func ExternalServiceLogic(es *nodes.ExternalService, payload map[string]any) map[string]any {
	requestID := payload["request_id"]
	mode := common.MustString(es.RunConfig, "external_service_mode")

	response := map[string]any{
		"request_id": payload["request_id"],
		"status":     "ok",
		"mode":       mode,
	}

	switch mode {
	case "idempotent":
		response["value"] = idempotentValueForRequest(requestID)
	case "side_effect":
		response["value"] = currentMonotonicValue()
		response["num_calls"] = sideEffectCallCount.Add(1)
	case "no_side_effect":
		response["value"] = currentMonotonicValue()
	}
	return response
}

func idempotentValueForRequest(requestID any) float64 {
	key := fmt.Sprintf("%v", requestID)
	idempotentMu.Lock()
	defer idempotentMu.Unlock()

	if value, ok := idempotentByRequestID[key]; ok {
		return value
	} else {
		value := currentMonotonicValue()
		idempotentByRequestID[key] = value
		return value
	}
}

func currentMonotonicValue() float64 {
	monotonicMu.RLock()
	defer monotonicMu.RUnlock()
	return monotonicValue
}

func resetMonotonicUpdater() {
	monotonicMu.Lock()
	if monotonicCancel != nil {
		monotonicCancel()
	}
	monotonicValue = 0
	stopCh := make(chan struct{})
	monotonicCancel = func() {
		close(stopCh)
	}
	monotonicMu.Unlock()

	go runMonotonicUpdater(stopCh)
}

func runMonotonicUpdater(stopCh <-chan struct{}) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			monotonicMu.Lock()
			monotonicValue += rand.Float64()
			monotonicMu.Unlock()
		case <-stopCh:
			return
		}
	}
}
