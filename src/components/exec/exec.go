package exec

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"aegean/common"
)

type pendingResponse struct {
	outputs    []map[string]any
	state      map[string]string
	merkle     *MerkleTree
	merkleRoot string
	token      string
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
	// Hard-coded fault parameters for now.
	u int
	r int
	// Current execution view.
	view int
	// Quorum + dedupe for verify responses from verifiers.
	verifyResponseQuorum *common.QuorumHelper
	verifyResponseMsgs   map[string]map[string]any // response tuple key -> payload
	verifyResponseBySeq  map[int]map[string]struct{}
	// Checkpoints for rollback to agreed (n, T).
	checkpoints map[int]rollbackCheckpoint
	// Timeout for unresolved verifier responses.
	verifyResponseTimeout time.Duration
	verifyResponseTimers  map[int]*time.Timer
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
	initialMerkle := NewMerkleTreeFromMap(initialKV)
	stable := State{
		KVStore:    common.CopyStringMap(initialKV),
		Merkle:     initialMerkle.Clone(),
		MerkleRoot: initialMerkle.Root(),
		SeqNum:     0,
		PrevHash:   strings.Repeat("0", 64),
		Verified:   true,
	}
	working := State{
		KVStore:    common.CopyStringMap(initialKV),
		Merkle:     initialMerkle.Clone(),
		MerkleRoot: initialMerkle.Root(),
		SeqNum:     0,
		PrevHash:   stable.PrevHash,
		Verified:   false,
	}
	exec := &Exec{
		Name:                  name,
		Verifiers:             verifiers,
		Peers:                 peers,
		VerifierCh:            verifierCh,
		ShimCh:                shimCh,
		ExecuteRequest:        executeRequest,
		stableState:           stable,
		workingState:          working,
		pendingResponses:      make(map[int]pendingResponse),
		u:                     1,
		r:                     0,
		view:                  1,
		verifyResponseMsgs:    make(map[string]map[string]any),
		verifyResponseBySeq:   make(map[int]map[string]struct{}),
		checkpoints:           make(map[int]rollbackCheckpoint),
		verifyResponseTimeout: 2 * time.Second,
		verifyResponseTimers:  make(map[int]*time.Timer),
		batchBuffer:           common.NewOOOBuffer[map[string]any](),
		verifyBuffer:          common.NewOOOBuffer[map[string]any](),
		nextBatchSeq:          1,
		nextVerifySeq:         1,
		workerCount:           4,
	}
	exec.verifyResponseQuorum = common.NewQuorumHelper(exec.r + 1)
	exec.storeCheckpoint(0, stable.PrevHash, stable.KVStore, stable.Merkle, stable.MerkleRoot)
	exec.scheduler = newExecScheduler()
	return exec
}

func responseTupleKey(view int, seqNum int, token string, forceSequential bool) string {
	return fmt.Sprintf("%d|%d|%s|%t", view, seqNum, token, forceSequential)
}

func (e *Exec) ReadKV(key string) string {
	e.stateMu.RLock()
	defer e.stateMu.RUnlock()
	e.workingState.EnsureMerkle()
	return e.workingState.Merkle.Get(key)
}

func (e *Exec) WriteKV(key, value string) {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	e.workingState.EnsureMerkle()
	e.workingState.Merkle.Set(key, value)
	e.workingState.KVStore = e.workingState.Merkle.SnapshotMap()
	e.workingState.MerkleRoot = e.workingState.Merkle.Root()
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

func (e *Exec) ConsumeNestedResponse(requestID any) (map[string]any, bool) {
	canonicalID, ok := canonicalRequestID(requestID)
	if !ok {
		return nil, false
	}
	return e.scheduler.popNestedResponse(canonicalID)
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
