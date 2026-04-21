package eo

import (
	"time"

	raftpb "go.etcd.io/raft/v3/raftpb"
)

const (
	// MessageTypeRaft identifies internal EO raft traffic on the wire.
	MessageTypeRaft = "eo_raft"
	raftMessageKey  = "raft_message"
)

// Entry is the ordered value replicated through the consensus box.
type Entry struct {
	RequestID string         `json:"request_id"`
	Response  map[string]any `json:"response"`
}

// AppliedEntry is delivered to the application once a committed slot becomes
// contiguous and safe to apply.
type AppliedEntry struct {
	Slot  uint64 `json:"slot"`
	Entry Entry  `json:"entry"`
}

// ExecuteFunc runs the request on the primary before the result is proposed.
type ExecuteFunc func(requestID string, request map[string]any) (map[string]any, error)

// ApplyFunc receives entries once EO has committed and applied them in slot order.
type ApplyFunc func(entry AppliedEntry)

// ForwardFunc forwards a client request to the current primary.
type ForwardFunc func(primary string, requestID string, request map[string]any) error

// SendRaftFunc ships a raft protocol message to a remote peer.
type SendRaftFunc func(peer string, message raftpb.Message) error

// CommitFunc is invoked by the consensus box whenever a replicated entry is committed.
type CommitFunc func(slot uint64, entry Entry)

// ConsensusBox is the ordered replication abstraction used by EO.
type ConsensusBox interface {
	IsPrimary() bool
	Primary() (string, bool)
	Propose(entry Entry) error
	HandleMessage(message raftpb.Message) error
	Stop()
}

// BoxConfig controls the raft-backed consensus box.
type BoxConfig struct {
	Name            string
	Peers           []string
	SendRaft        SendRaftFunc
	TickInterval    time.Duration
	ElectionTick    int
	HeartbeatTick   int
	MaxInflightMsgs int
	MaxSizePerMsg   uint64
}

// BoxFactory allows tests to swap in a fake consensus implementation.
type BoxFactory func(cfg BoxConfig, onCommit CommitFunc) (ConsensusBox, error)

// Config constructs an EO instance.
type Config struct {
	Name            string
	Peers           []string
	Execute         ExecuteFunc
	Apply           ApplyFunc
	Forward         ForwardFunc
	SendRaft        SendRaftFunc
	BoxFactory      BoxFactory
	TickInterval    time.Duration
	ElectionTick    int
	HeartbeatTick   int
	MaxInflightMsgs int
	MaxSizePerMsg   uint64
}
