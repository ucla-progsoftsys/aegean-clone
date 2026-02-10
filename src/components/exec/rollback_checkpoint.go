package exec

type rollbackCheckpoint struct {
	SeqNum         int
	Token          string
	Merkle         *MerkleTree
	MerkleRoot     string
	ValidationHash string
}

func (e *Exec) storeCheckpoint(seqNum int, token string, merkle *MerkleTree, merkleRoot string) {
	merkleCopy := NewMerkleTreeFromMap(nil)
	if merkle != nil {
		merkleCopy = merkle.Clone()
	}
	if merkleRoot == "" {
		merkleRoot = merkleCopy.Root()
	}
	e.checkpoints[seqNum] = rollbackCheckpoint{
		SeqNum:         seqNum,
		Token:          token,
		Merkle:         merkleCopy,
		MerkleRoot:     merkleRoot,
		ValidationHash: computeCheckpointValidationHash(seqNum, token, merkleRoot),
	}
}
