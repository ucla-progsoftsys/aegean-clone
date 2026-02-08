package exec

import (
	"sync"

	"aegean/common"
)

type execScheduler struct {
	mu               sync.Mutex
	inflightRequests map[string]*scheduledRequest
	nestedResponses  map[string][]map[string]any
	nestedReadyCh    chan struct{}
}

func newExecScheduler() *execScheduler {
	return &execScheduler{
		inflightRequests: make(map[string]*scheduledRequest),
		nestedResponses:  make(map[string][]map[string]any),
		nestedReadyCh:    make(chan struct{}, 1),
	}
}

func (s *execScheduler) enqueueNestedResponse(requestID string, payload map[string]any) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.inflightRequests[requestID]; !exists {
		return false
	}
	s.nestedResponses[requestID] = append(s.nestedResponses[requestID], payload)
	select {
	case s.nestedReadyCh <- struct{}{}:
	default:
	}
	return true
}

func (e *Exec) executeParallelBatch(requests []map[string]any, ndSeed int64, ndTimestamp float64) []map[string]any {
	return e.scheduler.executeParallelBatch(e, requests, ndSeed, ndTimestamp)
}

func (s *execScheduler) executeParallelBatch(e *Exec, requests []map[string]any, ndSeed int64, ndTimestamp float64) []map[string]any {
	if len(requests) == 0 {
		return nil
	}

	scheduled := make([]*scheduledRequest, 0, len(requests))
	for i, req := range requests {
		scheduled = append(scheduled, &scheduledRequest{
			index:   i,
			id:      requestIDForSchedule(req, i),
			payload: req,
			state:   requestRunnable,
		})
	}
	s.registerScheduledRequests(scheduled)
	defer s.unregisterScheduledRequests(scheduled)

	taskCh := make(chan *scheduledRequest, len(scheduled))
	resultCh := make(chan workerResult, len(scheduled))
	// TODO(perf): Reuse a persistent worker pool across batches to avoid
	// per-batch goroutine/channel setup overhead.

	workerCount := e.workerCount
	if workerCount <= 0 {
		workerCount = 1
	}
	if workerCount > len(scheduled) {
		workerCount = len(scheduled)
	}
	for i := 0; i < workerCount; i++ {
		go func() {
			for req := range taskCh {
				output := e.ExecuteRequest(e, req.payload, ndSeed, ndTimestamp)
				resultCh <- workerResult{req: req, output: output}
			}
		}()
	}

	total := len(scheduled)
	finished := 0
	activeWorkers := 0
	next := 0

	for finished < total {
		dispatched := false
		scanned := 0
		// TODO(perf): Avoid full round-robin scans when many requests are waiting
		// by tracking runnable/waiting indexes explicitly.
		for activeWorkers < workerCount && scanned < total {
			req := scheduled[next]
			next = (next + 1) % total
			scanned++

			switch req.state {
			case requestFinished, requestExecuting:
				// Skip; either done or already running.
			case requestWaiting:
				if !s.attachNestedResponse(req) {
					continue
				}
				req.state = requestRunnable
				taskCh <- req
				req.state = requestExecuting
				activeWorkers++
				dispatched = true
			case requestRunnable:
				taskCh <- req
				req.state = requestExecuting
				activeWorkers++
				dispatched = true
			}
		}

		if finished >= total {
			break
		}

		if activeWorkers > 0 {
			res := <-resultCh
			activeWorkers--
			status := common.GetString(res.output, "status")
			if status == "blocked_for_nested_response" {
				res.req.state = requestWaiting
			} else {
				res.req.state = requestFinished
				res.req.output = res.output
				finished++
			}
			continue
		}

		if !dispatched {
			<-s.nestedReadyCh
		}
	}

	close(taskCh)

	outputs := make([]map[string]any, 0, len(scheduled))
	for _, req := range scheduled {
		if req.output != nil {
			outputs = append(outputs, req.output)
		}
	}
	return outputs
}

func (s *execScheduler) registerScheduledRequests(requests []*scheduledRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, req := range requests {
		s.inflightRequests[req.id] = req
	}
}

func (s *execScheduler) unregisterScheduledRequests(requests []*scheduledRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, req := range requests {
		delete(s.inflightRequests, req.id)
		delete(s.nestedResponses, req.id)
	}
}

func (s *execScheduler) attachNestedResponse(req *scheduledRequest) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	queue := s.nestedResponses[req.id]
	if len(queue) == 0 {
		return false
	}
	req.payload["__nested_response"] = queue[0]
	if len(queue) == 1 {
		delete(s.nestedResponses, req.id)
	} else {
		s.nestedResponses[req.id] = queue[1:]
	}
	return true
}
