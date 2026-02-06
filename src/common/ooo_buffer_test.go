package common

import "testing"

// Pop on an empty buffer returns nil
func TestOOOBufferPopEmpty(t *testing.T) {
	buf := NewOOOBuffer[int]()
	if got := buf.Pop(1); got != nil {
		t.Fatalf("expected nil for empty pop, got %v", got)
	}
}

// Pop returns all items for a seq and clears them
func TestOOOBufferAddPopClears(t *testing.T) {
	buf := NewOOOBuffer[string]()
	buf.Add(2, "a")
	buf.Add(2, "b")

	got := buf.Pop(2)
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("unexpected pop result: %v", got)
	}

	if again := buf.Pop(2); again != nil {
		t.Fatalf("expected nil after pop, got %v", again)
	}
}

// Drop removes seqs <= cutoff and keeps higher ones
func TestOOOBufferDrop(t *testing.T) {
	buf := NewOOOBuffer[int]()
	buf.Add(1, 10)
	buf.Add(2, 20)
	buf.Add(3, 30)

	buf.Drop(2)

	if got := buf.Pop(1); got != nil {
		t.Fatalf("expected seq 1 dropped, got %v", got)
	}
	if got := buf.Pop(2); got != nil {
		t.Fatalf("expected seq 2 dropped, got %v", got)
	}
	if got := buf.Pop(3); len(got) != 1 || got[0] != 30 {
		t.Fatalf("expected seq 3 preserved, got %v", got)
	}
}
