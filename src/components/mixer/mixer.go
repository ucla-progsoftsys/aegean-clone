package mixer

import (
	"aegean/common"
	"aegean/telemetry"
	"context"
)

type Mixer struct {
	Name   string
	NextCh chan<- map[string]any
}

type socialMixerMode string

const (
	socialMixerModeConservativeHomeFanout socialMixerMode = "conservative_home_fanout"
	socialMixerModeNoHomeFanoutKey        socialMixerMode = "no_home_fanout_key"
	activeSocialMixerMode                 socialMixerMode = socialMixerModeNoHomeFanoutKey
)

func NewMixer(name string, nextCh chan<- map[string]any) *Mixer {
	if nextCh == nil {
		panic("mixer component requires non-nil nextCh")
	}
	m := &Mixer{
		Name:   name,
		NextCh: nextCh,
	}
	return m
}

func (m *Mixer) getKeys(request map[string]any) (map[string]struct{}, map[string]struct{}) {
	op, _ := request["op"].(string)
	payload, _ := request["op_payload"].(map[string]any)

	readKeys := make(map[string]struct{})
	writeKeys := make(map[string]struct{})

	switch op {
	case "read":
		if key, ok := payload["key"].(string); ok {
			readKeys[key] = struct{}{}
		}
	case "write":
		if key, ok := payload["key"].(string); ok {
			writeKeys[key] = struct{}{}
		}
	case "read_write":
		if key, ok := payload["read_key"].(string); ok {
			readKeys[key] = struct{}{}
		}
		if key, ok := payload["write_key"].(string); ok {
			writeKeys[key] = struct{}{}
		}
	case "spin_write_read":
		if key, ok := payload["read_key"].(string); ok {
			readKeys[key] = struct{}{}
		}
		if key, ok := payload["write_key"].(string); ok {
			writeKeys[key] = struct{}{}
		}
	case "write_user_timeline":
		if userID, ok := payload["user_id"].(string); ok && userID != "" {
			writeKeys["user_timeline:"+userID] = struct{}{}
		}
	case "read_user_timeline", "ro_read_user_timeline":
		if userID, ok := payload["user_id"].(string); ok && userID != "" {
			readKeys["user_timeline:"+userID] = struct{}{}
		}
	case "write_home_timeline":
		switch activeSocialMixerMode {
		case socialMixerModeConservativeHomeFanout:
			if userID, ok := payload["user_id"].(string); ok && userID != "" {
				writeKeys["home_timeline_fanout:"+userID] = struct{}{}
			}
		case socialMixerModeNoHomeFanoutKey:
			// Keep social keys for the other ops, but intentionally skip a mixer key
			// for write_home_timeline to maximize overlap on the fanout stage.
		}
	case "read_home_timeline", "ro_read_home_timeline":
		if userID, ok := payload["user_id"].(string); ok && userID != "" {
			readKeys["home_timeline:"+userID] = struct{}{}
		}
	case "store_post":
		if postID, ok := payload["post_id"].(string); ok && postID != "" {
			writeKeys["post:"+postID] = struct{}{}
		}
	case "read_post", "ro_read_post":
		if postID, ok := payload["post_id"].(string); ok && postID != "" {
			readKeys["post:"+postID] = struct{}{}
		}
	case "read_posts", "ro_read_posts":
		switch typed := payload["post_ids"].(type) {
		case []any:
			for _, raw := range typed {
				if postID, ok := raw.(string); ok && postID != "" {
					readKeys["post:"+postID] = struct{}{}
				}
			}
		case []string:
			for _, postID := range typed {
				if postID != "" {
					readKeys["post:"+postID] = struct{}{}
				}
			}
		}
	}

	return readKeys, writeKeys
}

func hasIntersection(a, b map[string]struct{}) bool {
	for key := range a {
		if _, ok := b[key]; ok {
			return true
		}
	}
	return false
}

func (m *Mixer) hasConflict(reqRead, reqWrite, batchReads, batchWrites map[string]struct{}) bool {
	if hasIntersection(reqWrite, batchWrites) {
		return true
	}
	if hasIntersection(reqWrite, batchReads) {
		return true
	}
	if hasIntersection(reqRead, batchWrites) {
		return true
	}
	return false
}

type parallelBatch struct {
	requests []map[string]any
	reads    map[string]struct{}
	writes   map[string]struct{}
}

func (m *Mixer) partitionIntoParallelBatches(batch []map[string]any) [][]map[string]any {
	parallelBatches := []parallelBatch{}

	for _, request := range batch {
		reqRead, reqWrite := m.getKeys(request)
		placed := false

		for i := range parallelBatches {
			pb := &parallelBatches[i]
			if !m.hasConflict(reqRead, reqWrite, pb.reads, pb.writes) {
				pb.requests = append(pb.requests, request)
				for key := range reqRead {
					pb.reads[key] = struct{}{}
				}
				for key := range reqWrite {
					pb.writes[key] = struct{}{}
				}
				placed = true
				break
			}
		}

		if !placed {
			pb := parallelBatch{
				requests: []map[string]any{request},
				reads:    reqRead,
				writes:   reqWrite,
			}
			parallelBatches = append(parallelBatches, pb)
		}
	}

	result := make([][]map[string]any, 0, len(parallelBatches))
	for _, pb := range parallelBatches {
		result = append(result, pb.requests)
	}
	return result
}

func (m *Mixer) HandleBatchMessage(payload map[string]any) map[string]any {
	ctx := telemetry.ExtractContext(context.Background(), payload)

	seqNum := common.GetInt(payload, "seq_num")
	requests, ok := normalizeRequestSlice(payload["requests"])
	if !ok {
		return map[string]any{"status": "error", "error": "invalid requests"}
	}

	parallelBatches := m.partitionIntoParallelBatches(requests)

	message := map[string]any{
		"type":             "batch",
		"seq_num":          seqNum,
		"parallel_batches": parallelBatches,
		"nd_seed":          payload["nd_seed"],
		"nd_timestamp":     payload["nd_timestamp"],
	}
	telemetry.InjectContext(ctx, message)

	if m.NextCh != nil {
		m.NextCh <- message
	} else {
	}

	return map[string]any{"status": "mixed", "seq_num": seqNum}
}

func normalizeRequestSlice(v any) ([]map[string]any, bool) {
	switch typed := v.(type) {
	case []map[string]any:
		return typed, true
	case []any:
		out := make([]map[string]any, 0, len(typed))
		for _, item := range typed {
			req, ok := item.(map[string]any)
			if !ok {
				return nil, false
			}
			out = append(out, req)
		}
		return out, true
	default:
		return nil, false
	}
}
