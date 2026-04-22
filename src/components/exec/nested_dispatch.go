package exec

import (
	"context"
	"sort"
	"sync"

	netx "aegean/net"
	"aegean/telemetry"
)

type NestedEOReplicator interface {
	IsPrimary() bool
	Primary() (string, bool)
	ProposeResponsePayload(requestID string, payload map[string]any) error
}

type nestedDispatchState uint8

const (
	nestedDispatchPending nestedDispatchState = iota
	nestedDispatchProposed
	nestedDispatchCompleted
)

type nestedDispatchTracker struct {
	mu     sync.Mutex
	states map[string]nestedDispatchState
}

func newNestedDispatchTracker() *nestedDispatchTracker {
	return &nestedDispatchTracker{
		states: make(map[string]nestedDispatchState),
	}
}

func (e *Exec) SetNestedEO(replication NestedEOReplicator) {
	e.nestedEO = replication
}

func (e *Exec) NestedEOReady() bool {
	if e.nestedEO == nil {
		return true
	}
	_, ok := e.nestedEO.Primary()
	return ok
}

func (e *Exec) DispatchNestedRequestDirect(sourceRequest map[string]any, targets []string, outgoing map[string]any) {
	if len(targets) == 0 || outgoing == nil {
		return
	}

	dispatchNestedRequestDirect(targets, e.prepareNestedRequest(sourceRequest, outgoing))
}

func (e *Exec) DispatchNestedRequestEO(sourceRequest map[string]any, targets []string, outgoing map[string]any) {
	if len(targets) == 0 || outgoing == nil || e.nestedEO == nil {
		return
	}

	prepared := e.prepareNestedRequest(sourceRequest, outgoing)

	requestID, ok := canonicalRequestID(prepared["request_id"])
	if !ok {
		dispatchNestedRequestDirect(targets, prepared)
		return
	}

	firstDispatch := e.nestedDispatchTracker.registerPending(requestID)
	if !firstDispatch {
		return
	}

	if e.nestedEO.IsPrimary() {
		dispatchNestedRequestDirect(targets, prepared)
		return
	}

	if _, ok := e.nestedEO.Primary(); !ok {
		dispatchNestedRequestDirect(targets, prepared)
	}
}

func (e *Exec) HandleNestedResponseMessage(payload map[string]any) (map[string]any, bool) {
	if e.nestedEO == nil || payload == nil {
		return nil, false
	}

	requestID, ok := canonicalRequestID(payload["request_id"])
	if !ok {
		return nil, false
	}

	state, tracked := e.nestedDispatchTracker.state(requestID)
	if !tracked {
		return nil, false
	}

	switch state {
	case nestedDispatchCompleted:
		return map[string]any{"status": "eo_nested_response_ignored", "request_id": requestID}, true
	case nestedDispatchPending:
		if !e.nestedEO.IsPrimary() {
			if _, ok := e.nestedEO.Primary(); ok {
				return map[string]any{"status": "eo_nested_response_waiting_for_primary", "request_id": requestID}, true
			}
			return nil, false
		}

		if !e.nestedDispatchTracker.markProposed(requestID) {
			return map[string]any{"status": "eo_nested_response_ignored", "request_id": requestID}, true
		}

		proposed := cloneMapAny(payload)
		proposed["shim_quorum_aggregated"] = true
		if err := e.nestedEO.ProposeResponsePayload(requestID, proposed); err != nil {
			e.nestedDispatchTracker.resetPending(requestID)
			return nil, false
		}
		return map[string]any{"status": "eo_nested_response_proposed", "request_id": requestID}, true
	case nestedDispatchProposed:
		return map[string]any{"status": "eo_nested_response_already_proposed", "request_id": requestID}, true
	default:
		return nil, false
	}
}

func (e *Exec) BufferExactOnceNestedResponse(payload map[string]any) bool {
	if payload == nil {
		return false
	}
	requestID, ok := canonicalRequestID(payload["request_id"])
	if ok {
		e.nestedDispatchTracker.markCompleted(requestID)
	}
	buffered := e.BufferNestedResponse(payload)
	if ok {
		e.nestedDispatchTracker.markCompleted(requestID)
	}
	return buffered
}

func (e *Exec) prepareNestedRequest(sourceRequest map[string]any, outgoing map[string]any) map[string]any {
	prepared := cloneMapAny(outgoing)
	prepared["sender"] = e.Name
	telemetry.InjectContext(telemetry.ExtractContext(context.Background(), sourceRequest), prepared)
	return prepared
}

func dispatchNestedRequestDirect(targets []string, outgoing map[string]any) {
	serviceTargets := append([]string{}, targets...)
	sort.Strings(serviceTargets)
	for _, target := range serviceTargets {
		duplicated := cloneMapAny(outgoing)
		go func(target string, outgoing map[string]any) {
			_, _ = netx.SendMessage(target, 8000, outgoing)
		}(target, duplicated)
	}
}

func (t *nestedDispatchTracker) registerPending(requestID string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.states[requestID]; exists {
		return false
	}
	t.states[requestID] = nestedDispatchPending
	return true
}

func (t *nestedDispatchTracker) state(requestID string) (nestedDispatchState, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	state, ok := t.states[requestID]
	return state, ok
}

func (t *nestedDispatchTracker) markProposed(requestID string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if state, ok := t.states[requestID]; !ok || state != nestedDispatchPending {
		return false
	}
	t.states[requestID] = nestedDispatchProposed
	return true
}

func (t *nestedDispatchTracker) resetPending(requestID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if state, ok := t.states[requestID]; ok && state == nestedDispatchProposed {
		t.states[requestID] = nestedDispatchPending
	}
}

func (t *nestedDispatchTracker) markCompleted(requestID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.states[requestID] = nestedDispatchCompleted
}
