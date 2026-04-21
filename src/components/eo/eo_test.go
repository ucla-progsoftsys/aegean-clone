package eo

import (
	"fmt"
	"sync"
	"testing"
	"time"

	raftpb "go.etcd.io/raft/v3/raftpb"
)

type fakeConsensusBox struct {
	isPrimary bool
	primary   string
	proposals []Entry
}

func (f *fakeConsensusBox) IsPrimary() bool {
	return f.isPrimary
}

func (f *fakeConsensusBox) Primary() (string, bool) {
	if f.primary == "" {
		return "", false
	}
	return f.primary, true
}

func (f *fakeConsensusBox) Propose(entry Entry) error {
	f.proposals = append(f.proposals, entry)
	return nil
}

func (f *fakeConsensusBox) HandleMessage(message raftpb.Message) error {
	return nil
}

func (f *fakeConsensusBox) Stop() {}

func newTestEO(t *testing.T, box ConsensusBox, execute ExecuteFunc, apply ApplyFunc, forward ForwardFunc) *EO {
	t.Helper()

	component, err := NewEO(Config{
		Name:    "node1",
		Peers:   []string{"node1", "node2", "node3"},
		Execute: execute,
		Apply:   apply,
		Forward: forward,
		BoxFactory: func(cfg BoxConfig, onCommit CommitFunc) (ConsensusBox, error) {
			return box, nil
		},
	})
	if err != nil {
		t.Fatalf("NewEO error: %v", err)
	}
	return component
}

func TestEOHandleRequestPrimaryDeduplicates(t *testing.T) {
	box := &fakeConsensusBox{isPrimary: true, primary: "node1"}
	executions := 0

	component := newTestEO(t, box, func(requestID string, request map[string]any) (map[string]any, error) {
		executions++
		return map[string]any{"status": "ok", "request_id": requestID}, nil
	}, nil, nil)
	defer component.Stop()

	first := component.HandleRequestMessage(map[string]any{"request_id": "r1", "op": "write"})
	second := component.HandleRequestMessage(map[string]any{"request_id": "r1", "op": "write"})

	if first["status"] != "proposed" {
		t.Fatalf("expected first request to be proposed, got %v", first["status"])
	}
	if second["status"] != "duplicate_request" {
		t.Fatalf("expected duplicate request to be ignored, got %v", second["status"])
	}
	if executions != 1 {
		t.Fatalf("expected execute callback once, got %d", executions)
	}
	if len(box.proposals) != 1 {
		t.Fatalf("expected one proposal, got %d", len(box.proposals))
	}
	if box.proposals[0].RequestID != "r1" {
		t.Fatalf("expected request_id r1, got %q", box.proposals[0].RequestID)
	}
}

func TestEOHandleRequestFollowerForwardsToPrimary(t *testing.T) {
	box := &fakeConsensusBox{isPrimary: false, primary: "node2"}
	var forwardedPrimary string
	var forwardedRequestID string
	var forwardedRequest map[string]any

	component := newTestEO(t, box, func(requestID string, request map[string]any) (map[string]any, error) {
		t.Fatalf("execute should not be called on follower")
		return nil, nil
	}, nil, func(primary string, requestID string, request map[string]any) error {
		forwardedPrimary = primary
		forwardedRequestID = requestID
		forwardedRequest = request
		return nil
	})
	defer component.Stop()

	response := component.HandleRequestMessage(map[string]any{"request_id": "r2", "op": "write"})

	if response["status"] != "forwarded_to_primary" {
		t.Fatalf("expected forwarded_to_primary, got %v", response["status"])
	}
	if forwardedPrimary != "node2" {
		t.Fatalf("expected primary node2, got %q", forwardedPrimary)
	}
	if forwardedRequestID != "r2" {
		t.Fatalf("expected request_id r2, got %q", forwardedRequestID)
	}
	if forwardedRequest["request_id"] != "r2" {
		t.Fatalf("expected forwarded request_id r2, got %v", forwardedRequest["request_id"])
	}
}

func TestEOTryApplyWaitsForContiguousCommit(t *testing.T) {
	box := &fakeConsensusBox{isPrimary: true, primary: "node1"}
	applied := make([]AppliedEntry, 0, 2)

	component := newTestEO(t, box, func(requestID string, request map[string]any) (map[string]any, error) {
		return map[string]any{"status": "ok"}, nil
	}, func(entry AppliedEntry) {
		applied = append(applied, entry)
	}, nil)
	defer component.Stop()

	component.OnCommit(2, Entry{RequestID: "r2", Response: map[string]any{"status": "two"}})
	if len(applied) != 0 {
		t.Fatalf("expected no applied entries yet, got %d", len(applied))
	}

	component.OnCommit(1, Entry{RequestID: "r1", Response: map[string]any{"status": "one"}})
	if len(applied) != 2 {
		t.Fatalf("expected two applied entries, got %d", len(applied))
	}
	if applied[0].Slot != 1 || applied[0].Entry.RequestID != "r1" {
		t.Fatalf("expected slot 1 / r1 first, got slot %d request %q", applied[0].Slot, applied[0].Entry.RequestID)
	}
	if applied[1].Slot != 2 || applied[1].Entry.RequestID != "r2" {
		t.Fatalf("expected slot 2 / r2 second, got slot %d request %q", applied[1].Slot, applied[1].Entry.RequestID)
	}
	if component.CommitIndex() != 2 {
		t.Fatalf("expected commit index 2, got %d", component.CommitIndex())
	}
	if component.AppliedIndex() != 2 {
		t.Fatalf("expected applied index 2, got %d", component.AppliedIndex())
	}
}

func TestEORaftClusterCommitsForwardedRequest(t *testing.T) {
	peers := []string{"node1", "node2", "node3"}
	appliedByNode := make(map[string]chan AppliedEntry, len(peers))
	nodes := make(map[string]*EO, len(peers))
	var nodesMu sync.RWMutex

	for _, name := range peers {
		appliedByNode[name] = make(chan AppliedEntry, 4)
	}

	for _, name := range peers {
		nodeName := name
		component, err := NewEO(Config{
			Name:  nodeName,
			Peers: peers,
			Execute: func(requestID string, request map[string]any) (map[string]any, error) {
				return map[string]any{
					"request_id": requestID,
					"status":     "ok",
					"value":      request["value"],
				}, nil
			},
			Apply: func(entry AppliedEntry) {
				appliedByNode[nodeName] <- entry
			},
			Forward: func(primary string, requestID string, request map[string]any) error {
				nodesMu.RLock()
				target := nodes[primary]
				nodesMu.RUnlock()
				if target == nil {
					return fmt.Errorf("primary %s unavailable", primary)
				}
				response := target.HandleRequest(requestID, request)
				if response["status"] == "proposal_error" || response["status"] == "execution_error" {
					return fmt.Errorf("%v", response["error"])
				}
				return nil
			},
			SendRaft: func(peer string, message raftpb.Message) error {
				nodesMu.RLock()
				target := nodes[peer]
				nodesMu.RUnlock()
				if target == nil {
					return fmt.Errorf("peer %s unavailable", peer)
				}
				payload, err := EncodeRaftMessage(message)
				if err != nil {
					return err
				}
				response := target.HandleRaftMessage(payload)
				if response["status"] != "raft_message_accepted" {
					return fmt.Errorf("peer %s rejected raft message: %v", peer, response["status"])
				}
				return nil
			},
			TickInterval:  5 * time.Millisecond,
			ElectionTick:  6,
			HeartbeatTick: 1,
		})
		if err != nil {
			t.Fatalf("NewEO(%s) error: %v", nodeName, err)
		}

		nodesMu.Lock()
		nodes[nodeName] = component
		nodesMu.Unlock()
	}
	defer func() {
		for _, component := range nodes {
			component.Stop()
		}
	}()

	leader := waitForLeader(t, nodes, 3*time.Second)
	var follower *EO
	for name, component := range nodes {
		if name != leader {
			follower = component
			break
		}
	}
	if follower == nil {
		t.Fatalf("expected at least one follower")
	}

	response := follower.HandleRequest("r-cluster", map[string]any{
		"request_id": "r-cluster",
		"value":      "payload",
	})
	if response["status"] != "forwarded_to_primary" {
		t.Fatalf("expected forwarded_to_primary, got %v", response["status"])
	}

	for _, name := range peers {
		select {
		case applied := <-appliedByNode[name]:
			if applied.Slot != 1 {
				t.Fatalf("%s expected slot 1, got %d", name, applied.Slot)
			}
			if applied.Entry.RequestID != "r-cluster" {
				t.Fatalf("%s expected request_id r-cluster, got %q", name, applied.Entry.RequestID)
			}
			if applied.Entry.Response["value"] != "payload" {
				t.Fatalf("%s expected payload response, got %v", name, applied.Entry.Response["value"])
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for applied entry on %s", name)
		}
	}
}

func waitForLeader(t *testing.T, nodes map[string]*EO, timeout time.Duration) string {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for name, component := range nodes {
			if component.IsPrimary() {
				return name
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for leader")
	return ""
}
