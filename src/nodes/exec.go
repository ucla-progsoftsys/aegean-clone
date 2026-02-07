package nodes

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"aegean/common"
)

type pendingResponse struct {
	outputs []map[string]any
	state   map[string]string
	token   string
	// verifySent indicates whether a verify message has been sent for this seq
	verifySent bool
}

// ExecuteRequestFunc handles a single request for an exec node
type ExecuteRequestFunc func(e *Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any

// ExecuteResponseFunc handles a response message for an exec node
type ExecuteResponseFunc func(e *Exec, payload map[string]any) map[string]any

type Exec struct {
	Name      string
	ExecID    string
	Verifiers []string
	Peers     []string
	// Local component channels
	VerifierCh chan<- map[string]any
	ShimCh     chan<- map[string]any
	LocalName  string
	kvStore    map[string]string
	mu         sync.Mutex
	// State management for rollback
	stableState  map[string]string
	stableSeqNum int
	prevHash     string
	// Pending responses (held until commit)
	pendingResponses map[int]pendingResponse
	// Sequential execution flag (set after rollback)
	forceSequential bool
	// Out-of-order buffers
	batchBuffer   *common.OOOBuffer[map[string]any]
	verifyBuffer  *common.OOOBuffer[map[string]any]
	nextBatchSeq  int
	nextVerifySeq int
	// Request execution hook
	ExecuteRequest ExecuteRequestFunc
	// Response handling hook
	HandleResponse ExecuteResponseFunc
	// Response sink for forwarding (e.g. shim)
	ResponseSink func(payload map[string]any) map[string]any
}

// TODO: request pipelining, parallel pipelining
// TODO: implement locking
func NewExec(name string, verifiers []string, peers []string, localName string, verifierCh chan<- map[string]any, shimCh chan<- map[string]any, executeRequest ExecuteRequestFunc, handleResponse ExecuteResponseFunc) *Exec {
	if verifierCh == nil || shimCh == nil {
		log.Fatalf("exec component requires non-nil channels")
	}
	if localName == "" {
		log.Fatalf("exec component requires localName")
	}
	if executeRequest == nil {
		log.Fatalf("exec component requires ExecuteRequest")
	}
	if handleResponse == nil {
		log.Fatalf("exec component requires HandleResponse")
	}
	exec := &Exec{
		Name:             name,
		ExecID:           name,
		Verifiers:        verifiers,
		Peers:            peers,
		LocalName:        localName,
		VerifierCh:       verifierCh,
		ShimCh:           shimCh,
		ExecuteRequest:   executeRequest,
		HandleResponse:   handleResponse,
		kvStore:          map[string]string{"1": "111"},
		stableSeqNum:     0,
		prevHash:         strings.Repeat("0", 64),
		pendingResponses: make(map[int]pendingResponse),
		batchBuffer:      common.NewOOOBuffer[map[string]any](),
		verifyBuffer:     common.NewOOOBuffer[map[string]any](),
		nextBatchSeq:     1,
		nextVerifySeq:    1,
	}
	exec.stableState = copyStringMap(exec.kvStore)
	return exec
}

func (e *Exec) computeStateHash(state map[string]string, outputs []map[string]any, prevHash string, seqNum int) string {
	// TODO: Merkle tree
	// Compute Merkle-tree-style hash of state and outputs
	data := map[string]any{
		"seq_num":   seqNum,
		"prev_hash": prevHash,
		"state":     state,
		"outputs":   outputs,
	}
	encoded := marshalSorted(data)
	hash := sha256.Sum256(encoded)
	return hex.EncodeToString(hash[:])
}

func (e *Exec) ReadKV(key string) string {
	return e.kvStore[key]
}

func (e *Exec) WriteKV(key, value string) {
	e.kvStore[key] = value
}

func (e *Exec) HandleBatchMessage(payload map[string]any) map[string]any {
	log.Printf("Handler called on %s with payload: %v", e.Name, payload)
	seqNum := getInt(payload, "seq_num")
	e.batchBuffer.Add(seqNum, payload)
	for {
		progressed := false
		if e.flushNextBatch() {
			progressed = true
		}
		if e.flushNextVerify() {
			progressed = true
		}
		if !progressed {
			break
		}
	}
	return map[string]any{"status": "buffered", "seq_num": seqNum}
}

func (e *Exec) HandleVerifyResponseMessage(payload map[string]any) map[string]any {
	log.Printf("Handler called on %s with payload: %v", e.Name, payload)
	seqNum := getInt(payload, "seq_num")
	e.verifyBuffer.Add(seqNum, payload)
	for {
		if !e.flushNextVerify() {
			break
		}
	}
	return map[string]any{"status": "buffered", "seq_num": seqNum}
}

func (e *Exec) HandleStateTransferRequestMessage(payload map[string]any) map[string]any {
	return e.handleStateTransferRequest(payload)
}

func (e *Exec) flushNextBatch() bool {
	msgs := e.batchBuffer.Pop(e.nextBatchSeq)
	if len(msgs) == 0 {
		return false
	}
	for _, msg := range msgs {
		_ = e.handleBatch(msg)
	}
	e.nextBatchSeq++
	return true
}

func (e *Exec) flushNextVerify() bool {
	e.mu.Lock()
	pending, ok := e.pendingResponses[e.nextVerifySeq]
	stableSeqNum := e.stableSeqNum
	e.mu.Unlock()
	if !ok {
		return false
	}
	if e.nextVerifySeq != stableSeqNum+1 {
		return false
	}
	// Compute token with committed prevHash to avoid divergence
	if !pending.verifySent {
		token := e.computeStateHash(pending.state, pending.outputs, e.prevHash, e.nextVerifySeq)
		pending.token = token
		pending.verifySent = true
		e.mu.Lock()
		e.pendingResponses[e.nextVerifySeq] = pending
		e.mu.Unlock()

		verifyMsg := map[string]any{
			"type":      "verify",
			"seq_num":   e.nextVerifySeq,
			"token":     token,
			"prev_hash": e.prevHash,
			"exec_id":   e.ExecID,
		}

		for _, verifier := range e.Verifiers {
			if verifier == e.LocalName && e.VerifierCh != nil {
				e.VerifierCh <- verifyMsg
				continue
			}
			if _, err := common.SendMessage(verifier, 8000, verifyMsg); err != nil {
				log.Printf("Failed to send to verifier %s: %v", verifier, err)
			}
		}
		return true
	}
	msgs := e.verifyBuffer.Pop(e.nextVerifySeq)
	if len(msgs) == 0 {
		return false
	}
	for _, msg := range msgs {
		_ = e.handleVerifyResponse(msg)
	}
	e.nextVerifySeq++
	return true
}

func (e *Exec) handleBatch(payload map[string]any) map[string]any {
	seqNum := getInt(payload, "seq_num")
	parallelBatchesAny, _ := payload["parallel_batches"]
	ndSeed := getInt64(payload, "nd_seed")
	ndTimestamp := getFloat(payload, "nd_timestamp")
	if ndTimestamp == 0 {
		ndTimestamp = float64(time.Now().UnixNano()) / 1e9
	}

	parallelBatches, ok := parallelBatchesAny.([][]map[string]any)
	if !ok {
		log.Printf("%s: Invalid parallel_batches type %T", e.Name, parallelBatchesAny)
		return map[string]any{"status": "error", "error": "invalid parallel_batches"}
	}
	log.Printf("%s: Executing batch %d with %d parallelBatches", e.Name, seqNum, len(parallelBatches))

	// Execute all parallelBatches and collect outputs
	outputs := make([]map[string]any, 0)
	for _, pbAny := range parallelBatches {
		for _, reqMap := range pbAny {
			// TODO: In prototype, execute sequentially within parallelBatch
			// (Real impl would use threading for parallel execution)
			output := e.ExecuteRequest(e, reqMap, ndSeed, ndTimestamp)
			outputs = append(outputs, output)
		}
	}

	// TODO: check that all requests in the batch are finished before verification phase
	e.mu.Lock()
	e.pendingResponses[seqNum] = pendingResponse{
		outputs: outputs,
		state:   copyStringMap(e.kvStore),
		token:   "",
	}
	e.mu.Unlock()
	return map[string]any{"status": "executed", "seq_num": seqNum}
}

func (e *Exec) handleVerifyResponse(payload map[string]any) map[string]any {
	decision, _ := payload["decision"].(string)
	seqNum := getInt(payload, "seq_num")
	agreedToken, _ := payload["token"].(string)

	e.mu.Lock()
	if seqNum <= e.stableSeqNum {
		e.mu.Unlock()
		return map[string]any{"status": "already_committed", "seq_num": seqNum}
	}
	pending, ok := e.pendingResponses[seqNum]
	if !ok {
		e.mu.Unlock()
		return map[string]any{"status": "no_pending_for_seq"}
	}

	// Handle verification response from verifier
	switch decision {
	case "commit":
		if pending.token == agreedToken {
			// Mark state as stable and release responses
			log.Printf("%s: Committing seq_num %d", e.Name, seqNum)
			e.stableState = copyStringMap(pending.state)
			e.stableSeqNum = seqNum
			e.forceSequential = false
			e.prevHash = agreedToken
			delete(e.pendingResponses, seqNum)
			outputs := pending.outputs
			e.mu.Unlock()

			// Send responses back to the server-shim for broadcasting to clients
			for _, output := range outputs {
				requestID := output["request_id"]
				responseMsg := map[string]any{
					"type":       "response",
					"request_id": requestID,
					"response":   output,
				}
				if e.ShimCh != nil {
					e.ShimCh <- responseMsg
				}
				log.Printf("%s: Sent response for request %v to shim", e.Name, requestID)
			}
			return map[string]any{"status": "processed", "decision": decision}
		} else {
			delete(e.pendingResponses, seqNum)
			e.mu.Unlock()
			// TODO: rollback? (I guess we need to introduce parallel pipelining first)
			// Our state diverged - need state transfer from a replica with correct state
			log.Printf("%s: State diverged at seq_num %d, requesting state transfer", e.Name, seqNum)
			if e.requestStateTransfer() {
				log.Printf("%s: State transfer successful for seq_num %d", e.Name, seqNum)
			} else {
				// If state transfer fails, fall back to rollback
				log.Printf("%s: State transfer failed, rolling back", e.Name)
				e.kvStore = copyStringMap(e.stableState)
				e.forceSequential = true
			}
		}
	case "rollback":
		log.Printf("%s: Rolling back to seq_num %d", e.Name, e.stableSeqNum)
		e.kvStore = copyStringMap(e.stableState)
		e.forceSequential = true
		delete(e.pendingResponses, seqNum)
		e.mu.Unlock()
		log.Printf("%s: Will execute next batch sequentially", e.Name)
		return map[string]any{"status": "processed", "decision": decision}
	default:
		e.mu.Unlock()
	}

	// Cleanup
	return map[string]any{"status": "processed", "decision": decision}
}

func (e *Exec) requestStateTransfer() bool {
	// TODO: should state transfer be async? Meaning that should state transfer request
	// spin and wait for a response before processing other requests
	// TODO: after state transfer, do we send back client the response?
	// Request state transfer from a replica that has the correct state
	for _, sourceExec := range e.Peers {
		log.Printf("%s: Requesting state transfer from %s", e.Name, sourceExec)
		requestMsg := map[string]any{
			"type":            "state_transfer_request",
			"requesting_exec": e.Name,
		}

		response, err := common.SendMessage(sourceExec, 8000, requestMsg)
		if err != nil {
			log.Printf("%s: Error requesting state transfer from %s: %v", e.Name, sourceExec, err)
			continue
		}

		if response == nil || response["status"] != "ok" {
			log.Printf("%s: State transfer from %s failed: %v", e.Name, sourceExec, response)
			continue
		}

		transferredState, ok := response["state"].(map[string]any)
		if !ok {
			log.Printf("%s: Invalid state transfer from %s", e.Name, sourceExec)
			continue
		}

		transferredStableSeqNum := getInt(response, "stable_seq_num")
		transferredPrevHash, _ := response["prev_hash"].(string)

		// Only apply if the provided stable seq num is higher than ours
		if transferredStableSeqNum <= e.stableSeqNum {
			log.Printf("%s: Received stable_seq_num %d from %s is not higher than ours (%d)", e.Name, transferredStableSeqNum, sourceExec, e.stableSeqNum)
			continue
		}

		// Apply the transferred state
		converted := make(map[string]string, len(transferredState))
		for key, value := range transferredState {
			converted[key] = fmt.Sprintf("%v", value)
		}

		e.kvStore = copyStringMap(converted)
		e.stableState = copyStringMap(converted)
		e.stableSeqNum = transferredStableSeqNum
		e.prevHash = transferredPrevHash
		e.forceSequential = false
		for seq := range e.pendingResponses {
			if seq <= e.stableSeqNum {
				delete(e.pendingResponses, seq)
			}
		}
		e.batchBuffer.Drop(e.stableSeqNum)
		e.verifyBuffer.Drop(e.stableSeqNum)
		if e.nextBatchSeq < e.stableSeqNum+1 {
			e.nextBatchSeq = e.stableSeqNum + 1
		}
		if e.nextVerifySeq < e.stableSeqNum+1 {
			e.nextVerifySeq = e.stableSeqNum + 1
		}

		log.Printf("%s: Successfully applied state transfer from %s, now at stable_seq_num %d", e.Name, sourceExec, e.stableSeqNum)
		return true
	}
	return false
}

func (e *Exec) handleStateTransferRequest(payload map[string]any) map[string]any {
	requestingExec, _ := payload["requesting_exec"].(string)
	log.Printf("%s: Received state transfer request from %s, providing stable state at seq_num %d", e.Name, requestingExec, e.stableSeqNum)

	return map[string]any{
		"status":         "ok",
		"state":          copyStringMap(e.stableState),
		"stable_seq_num": e.stableSeqNum,
		"prev_hash":      e.prevHash,
	}
}

func copyStringMap(input map[string]string) map[string]string {
	out := make(map[string]string, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func getFloat(m map[string]any, key string) float64 {
	if value, ok := m[key]; ok {
		switch v := value.(type) {
		case float64:
			return v
		case int:
			return float64(v)
		case int64:
			return float64(v)
		}
	}
	return 0
}

func getInt(m map[string]any, key string) int {
	if value, ok := m[key]; ok {
		switch v := value.(type) {
		case float64:
			return int(v)
		case int:
			return v
		case int64:
			return int(v)
		}
	}
	return 0
}

func getInt64(m map[string]any, key string) int64 {
	if value, ok := m[key]; ok {
		switch v := value.(type) {
		case float64:
			return int64(v)
		case int:
			return int64(v)
		case int64:
			return v
		}
	}
	return 0
}

// marshalSorted produces JSON with sorted keys to match Python's json.dumps(sort_keys=True)
func marshalSorted(v any) []byte {
	var buf bytes.Buffer
	writeSorted(&buf, v)
	return buf.Bytes()
}

func writeSorted(buf *bytes.Buffer, v any) {
	switch val := v.(type) {
	case map[string]any:
		buf.WriteByte('{')
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for i, k := range keys {
			if i > 0 {
				buf.WriteString(", ")
			}
			keyBytes, _ := json.Marshal(k)
			buf.Write(keyBytes)
			buf.WriteString(": ")
			writeSorted(buf, val[k])
		}
		buf.WriteByte('}')
	case map[string]string:
		buf.WriteByte('{')
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for i, k := range keys {
			if i > 0 {
				buf.WriteString(", ")
			}
			keyBytes, _ := json.Marshal(k)
			buf.Write(keyBytes)
			buf.WriteString(": ")
			valBytes, _ := json.Marshal(val[k])
			buf.Write(valBytes)
		}
		buf.WriteByte('}')
	case []any:
		buf.WriteByte('[')
		for i, item := range val {
			if i > 0 {
				buf.WriteString(", ")
			}
			writeSorted(buf, item)
		}
		buf.WriteByte(']')
	case []map[string]any:
		buf.WriteByte('[')
		for i, item := range val {
			if i > 0 {
				buf.WriteString(", ")
			}
			writeSorted(buf, item)
		}
		buf.WriteByte(']')
	default:
		encoded, _ := json.Marshal(val)
		buf.Write(encoded)
	}
}
