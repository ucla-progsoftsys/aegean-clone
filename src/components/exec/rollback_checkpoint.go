package exec

import "aegean/common"

type rollbackCheckpoint struct {
	SeqNum         int
	Token          string
	State          map[string]string
	Merkle         *MerkleTree
	MerkleRoot     string
	ValidationHash string
}

func (e *Exec) storeCheckpoint(seqNum int, token string, state map[string]string, merkle *MerkleTree, merkleRoot string) {
	stateCopy := common.CopyStringMap(state)
	merkleCopy := NewMerkleTreeFromMap(stateCopy)
	if merkle != nil {
		merkleCopy = merkle.Clone()
	}
	if merkleRoot == "" {
		merkleRoot = merkleCopy.Root()
	}
	e.checkpoints[seqNum] = rollbackCheckpoint{
		SeqNum:         seqNum,
		Token:          token,
		State:          stateCopy,
		Merkle:         merkleCopy,
		MerkleRoot:     merkleRoot,
		ValidationHash: computeCheckpointValidationHash(seqNum, token, merkleRoot),
	}
}
