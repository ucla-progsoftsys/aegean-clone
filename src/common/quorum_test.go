package common

import "testing"

// Reaches quorum exactly when the Nth unique sender is added
func TestQuorumHelperReachesQuorum(t *testing.T) {
	q := NewQuorumHelper(2)

	if got := q.Add("r1", "a"); got {
		t.Fatalf("expected no quorum after first sender")
	}
	if got := q.Add("r1", "b"); !got {
		t.Fatalf("expected quorum after second sender")
	}
}

// Duplicate senders do not advance quorum
func TestQuorumHelperIgnoresDuplicates(t *testing.T) {
	q := NewQuorumHelper(2)

	if got := q.Add("r1", "a"); got {
		t.Fatalf("expected no quorum after first sender")
	}
	if got := q.Add("r1", "a"); got {
		t.Fatalf("expected no quorum after duplicate sender")
	}
	if got := q.Add("r1", "b"); !got {
		t.Fatalf("expected quorum after second unique sender")
	}
}

// Distinct request IDs maintain independent quorum tracking
func TestQuorumHelperIndependentRequestIDs(t *testing.T) {
	q := NewQuorumHelper(2)

	if got := q.Add("r1", "a"); got {
		t.Fatalf("expected no quorum for r1 after first sender")
	}
	if got := q.Add("r2", "a"); got {
		t.Fatalf("expected no quorum for r2 after first sender")
	}
	if got := q.Add("r2", "b"); !got {
		t.Fatalf("expected quorum for r2 after second sender")
	}
	if got := q.Add("r1", "b"); !got {
		t.Fatalf("expected quorum for r1 after second sender")
	}
}
