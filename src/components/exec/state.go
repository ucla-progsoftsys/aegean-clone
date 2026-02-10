package exec

type State struct {
	KVStore    map[string]string
	Merkle     *MerkleTree
	MerkleRoot string
	SeqNum     int
	PrevHash   string
	Verified   bool
}

func (s *State) EnsureMerkle() {
	if s.KVStore == nil {
		s.KVStore = make(map[string]string)
	}
	if s.Merkle == nil {
		s.Merkle = NewMerkleTreeFromMap(s.KVStore)
		s.MerkleRoot = s.Merkle.Root()
		return
	}
	// Rebuild from KV to keep direct map assignments deterministic in prototype code/tests.
	s.Merkle = NewMerkleTreeFromMap(s.KVStore)
	s.MerkleRoot = s.Merkle.Root()
}
