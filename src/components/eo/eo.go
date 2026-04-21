package eo

import (
	"fmt"
	"sync"

	"aegean/common"
)

type EO struct {
	name    string
	execute ExecuteFunc
	apply   ApplyFunc
	forward ForwardFunc
	box     ConsensusBox

	mu       sync.Mutex
	log      map[uint64]Entry
	commit   uint64
	applied  uint64
	requests map[string]struct{}
}

func NewEO(cfg Config) (*EO, error) {
	if cfg.Name == "" {
		return nil, fmt.Errorf("eo requires a node name")
	}
	if len(cfg.Peers) == 0 {
		return nil, fmt.Errorf("eo requires at least one peer")
	}
	if cfg.Execute == nil {
		return nil, fmt.Errorf("eo requires an execute callback")
	}
	factory := cfg.BoxFactory
	if factory == nil {
		if cfg.SendRaft == nil {
			return nil, fmt.Errorf("eo requires a raft sender when using the default consensus box")
		}
		factory = newRaftConsensusBox
	}

	e := &EO{
		name:     cfg.Name,
		execute:  cfg.Execute,
		apply:    cfg.Apply,
		forward:  cfg.Forward,
		log:      make(map[uint64]Entry),
		requests: make(map[string]struct{}),
	}

	box, err := factory(BoxConfig{
		Name:            cfg.Name,
		Peers:           append([]string{}, cfg.Peers...),
		SendRaft:        cfg.SendRaft,
		TickInterval:    cfg.TickInterval,
		ElectionTick:    cfg.ElectionTick,
		HeartbeatTick:   cfg.HeartbeatTick,
		MaxInflightMsgs: cfg.MaxInflightMsgs,
		MaxSizePerMsg:   cfg.MaxSizePerMsg,
	}, e.OnCommit)
	if err != nil {
		return nil, err
	}
	e.box = box
	return e, nil
}

func (e *EO) Name() string {
	return e.name
}

func (e *EO) IsPrimary() bool {
	return e.box.IsPrimary()
}

func (e *EO) Primary() (string, bool) {
	return e.box.Primary()
}

func (e *EO) Stop() {
	if e.box != nil {
		e.box.Stop()
	}
}

func (e *EO) HandleMessage(payload map[string]any) map[string]any {
	if common.GetString(payload, "type") == MessageTypeRaft {
		return e.HandleRaftMessage(payload)
	}
	return e.HandleRequestMessage(payload)
}

func (e *EO) HandleRequestMessage(payload map[string]any) map[string]any {
	requestID, ok := canonicalRequestID(payload["request_id"])
	if !ok {
		return map[string]any{"status": "invalid_request", "error": "missing request_id"}
	}
	return e.HandleRequest(requestID, payload)
}

func (e *EO) HandleRequest(requestID string, request map[string]any) map[string]any {
	requestCopy := cloneMap(request)
	requestCopy["request_id"] = requestID

	if !e.box.IsPrimary() {
		primary, ok := e.box.Primary()
		if !ok || primary == "" {
			return map[string]any{
				"status":     "waiting_for_primary",
				"request_id": requestID,
			}
		}
		if e.forward == nil {
			return map[string]any{
				"status":     "not_primary",
				"request_id": requestID,
				"primary":    primary,
			}
		}
		if err := e.forward(primary, requestID, requestCopy); err != nil {
			return map[string]any{
				"status":     "forward_error",
				"request_id": requestID,
				"primary":    primary,
				"error":      err.Error(),
			}
		}
		return map[string]any{
			"status":     "forwarded_to_primary",
			"request_id": requestID,
			"primary":    primary,
		}
	}

	if !e.tryStartRequest(requestID) {
		return map[string]any{
			"status":     "duplicate_request",
			"request_id": requestID,
		}
	}

	response, err := e.execute(requestID, requestCopy)
	if err != nil {
		e.finishRequestFailure(requestID)
		return map[string]any{
			"status":     "execution_error",
			"request_id": requestID,
			"error":      err.Error(),
		}
	}
	if response == nil {
		response = map[string]any{}
	}

	entry := Entry{
		RequestID: requestID,
		Response:  response,
	}
	if err := e.box.Propose(entry); err != nil {
		e.finishRequestFailure(requestID)
		return map[string]any{
			"status":     "proposal_error",
			"request_id": requestID,
			"error":      err.Error(),
		}
	}

	return map[string]any{
		"status":     "proposed",
		"request_id": requestID,
	}
}

func (e *EO) HandleRaftMessage(payload map[string]any) map[string]any {
	message, err := DecodeRaftMessage(payload)
	if err != nil {
		return map[string]any{
			"status": "invalid_raft_message",
			"error":  err.Error(),
		}
	}
	if err := e.box.HandleMessage(message); err != nil {
		return map[string]any{
			"status": "raft_step_error",
			"error":  err.Error(),
		}
	}
	return map[string]any{"status": "raft_message_accepted"}
}

// OnCommit is the callback that the consensus box invokes when a slot commits.
func (e *EO) OnCommit(slot uint64, entry Entry) {
	e.mu.Lock()
	if _, exists := e.log[slot]; exists {
		e.mu.Unlock()
		return
	}
	e.log[slot] = entry
	if slot > e.commit {
		e.commit = slot
	}
	e.mu.Unlock()

	e.TryApply()
}

// TryApply best-effort applies every contiguous committed slot.
func (e *EO) TryApply() {
	appliedEntries := e.dequeueContiguousAppliedEntries()
	if e.apply == nil {
		return
	}
	for _, entry := range appliedEntries {
		e.apply(entry)
	}
}

func (e *EO) CommitIndex() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.commit
}

func (e *EO) AppliedIndex() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.applied
}

func (e *EO) Entry(slot uint64) (Entry, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	entry, ok := e.log[slot]
	return entry, ok
}

func (e *EO) tryStartRequest(requestID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, exists := e.requests[requestID]; exists {
		return false
	}
	e.requests[requestID] = struct{}{}
	return true
}

func (e *EO) finishRequestFailure(requestID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.requests, requestID)
}

func (e *EO) dequeueContiguousAppliedEntries() []AppliedEntry {
	e.mu.Lock()
	defer e.mu.Unlock()

	appliedEntries := make([]AppliedEntry, 0)
	for {
		nextSlot := e.applied + 1
		if nextSlot > e.commit {
			return appliedEntries
		}
		entry, ok := e.log[nextSlot]
		if !ok {
			return appliedEntries
		}
		e.applied = nextSlot
		appliedEntries = append(appliedEntries, AppliedEntry{
			Slot:  nextSlot,
			Entry: entry,
		})
	}
}

func cloneMap(input map[string]any) map[string]any {
	if input == nil {
		return map[string]any{}
	}
	out := make(map[string]any, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func canonicalRequestID(id any) (string, bool) {
	if id == nil {
		return "", false
	}
	return fmt.Sprintf("%v", id), true
}
