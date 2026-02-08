package exec

import (
	"log"
	"strings"
	"sync"

	"aegean/common"
)

type pendingResponse struct {
	outputs []map[string]any
	state   map[string]string
	token   string
	// verifySent indicates whether a verify message has been sent for this seq
	verifySent bool
}

// ExecuteRequestFunc handles a single request for an exec node.
type ExecuteRequestFunc func(e *Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any

type Exec struct {
	Name      string
	Verifiers []string
	Peers     []string
	// Local component channels
	VerifierCh chan<- map[string]any
	ShimCh     chan<- map[string]any
	mu         sync.Mutex
	stateMu    sync.RWMutex
	// State management for rollback
	stableState  State
	workingState State
	// Pending responses (held until commit)
	pendingResponses map[int]pendingResponse
	// Sequential execution flag (set after rollback)
	forceSequential bool
	// Out-of-order buffers
	batchBuffer   *common.OOOBuffer[map[string]any]
	verifyBuffer  *common.OOOBuffer[map[string]any]
	nextBatchSeq  int
	nextVerifySeq int
	workerCount   int
	scheduler     *execScheduler
	// Request execution hook
	ExecuteRequest ExecuteRequestFunc
}

// TODO: request pipelining, parallel pipelining
// TODO: implement locking
func NewExec(name string, verifiers []string, peers []string, verifierCh chan<- map[string]any, shimCh chan<- map[string]any, executeRequest ExecuteRequestFunc) *Exec {
	if verifierCh == nil || shimCh == nil {
		log.Fatalf("exec component requires non-nil channels")
	}
	if executeRequest == nil {
		log.Fatalf("exec component requires ExecuteRequest")
	}
	initialKV := map[string]string{"1": "111"}
	stable := State{
		KVStore:  common.CopyStringMap(initialKV),
		SeqNum:   0,
		PrevHash: strings.Repeat("0", 64),
		Verified: true,
	}
	working := State{
		KVStore:  initialKV,
		SeqNum:   0,
		PrevHash: stable.PrevHash,
		Verified: false,
	}
	exec := &Exec{
		Name:             name,
		Verifiers:        verifiers,
		Peers:            peers,
		VerifierCh:       verifierCh,
		ShimCh:           shimCh,
		ExecuteRequest:   executeRequest,
		stableState:      stable,
		workingState:     working,
		pendingResponses: make(map[int]pendingResponse),
		batchBuffer:      common.NewOOOBuffer[map[string]any](),
		verifyBuffer:     common.NewOOOBuffer[map[string]any](),
		nextBatchSeq:     1,
		nextVerifySeq:    1,
		workerCount:      4,
	}
	exec.scheduler = newExecScheduler()
	return exec
}

func (e *Exec) ReadKV(key string) string {
	e.stateMu.RLock()
	defer e.stateMu.RUnlock()
	return e.workingState.KVStore[key]
}

func (e *Exec) WriteKV(key, value string) {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	e.workingState.KVStore[key] = value
}

func (e *Exec) BufferNestedResponse(payload map[string]any) bool {
	if payload == nil {
		return false
	}
	requestIDRaw, ok := payload["request_id"]
	if !ok || requestIDRaw == nil {
		return false
	}
	requestID, ok := canonicalRequestID(requestIDRaw)
	if !ok {
		return false
	}
	return e.scheduler.enqueueNestedResponse(requestID, payload)
}

func (e *Exec) HandleBatchMessage(payload map[string]any) map[string]any {
	log.Printf("Handler called on %s with payload: %v", e.Name, payload)
	seqNum := common.GetInt(payload, "seq_num")
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
	seqNum := common.GetInt(payload, "seq_num")
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
