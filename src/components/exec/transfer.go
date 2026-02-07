package exec

import (
	"fmt"
	"log"

	"aegean/common"
)

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

		transferredStableSeqNum := common.GetInt(response, "stable_seq_num")
		transferredPrevHash, _ := response["prev_hash"].(string)

		// Only apply if the provided stable seq num is higher than ours
		if transferredStableSeqNum <= e.stableState.SeqNum {
			log.Printf("%s: Received stable_seq_num %d from %s is not higher than ours (%d)", e.Name, transferredStableSeqNum, sourceExec, e.stableState.SeqNum)
			continue
		}

		// Apply the transferred state
		converted := make(map[string]string, len(transferredState))
		for key, value := range transferredState {
			converted[key] = fmt.Sprintf("%v", value)
		}

		e.workingState.KVStore = common.CopyStringMap(converted)
		e.stableState = State{
			KVStore:  common.CopyStringMap(converted),
			SeqNum:   transferredStableSeqNum,
			PrevHash: transferredPrevHash,
			Verified: true,
		}
		e.forceSequential = false
		for seq := range e.pendingResponses {
			if seq <= e.stableState.SeqNum {
				delete(e.pendingResponses, seq)
			}
		}
		e.batchBuffer.Drop(e.stableState.SeqNum)
		e.verifyBuffer.Drop(e.stableState.SeqNum)
		if e.nextBatchSeq < e.stableState.SeqNum+1 {
			e.nextBatchSeq = e.stableState.SeqNum + 1
		}
		if e.nextVerifySeq < e.stableState.SeqNum+1 {
			e.nextVerifySeq = e.stableState.SeqNum + 1
		}

		log.Printf("%s: Successfully applied state transfer from %s, now at stable_seq_num %d", e.Name, sourceExec, e.stableState.SeqNum)
		return true
	}
	return false
}

func (e *Exec) handleStateTransferRequest(payload map[string]any) map[string]any {
	requestingExec, _ := payload["requesting_exec"].(string)
	log.Printf("%s: Received state transfer request from %s, providing stable state at seq_num %d", e.Name, requestingExec, e.stableState.SeqNum)

	return map[string]any{
		"status":         "ok",
		"state":          common.CopyStringMap(e.stableState.KVStore),
		"stable_seq_num": e.stableState.SeqNum,
		"prev_hash":      e.stableState.PrevHash,
	}
}
