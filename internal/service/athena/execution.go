package athena

import (
	"context"
	"encoding/json"
	"time"
)

// HTTP request cancellation does not cancel an accepted job; StopQueryExecution
// and service shutdown own the execution lifetime.
func (s *MemoryStorage) execute(ctx context.Context, request *QueryExecution) {
	defer s.workers.Done()
	s.mu.Lock()
	current, exists := s.QueryExecutions[request.QueryExecutionID]
	if !exists || current.Status.State == QueryExecutionStateCancelled || ctx.Err() != nil {
		delete(s.jobs, request.QueryExecutionID)
		s.mu.Unlock()
		return
	}
	current.Status.State = QueryExecutionStateRunning
	s.saveLocked()
	s.mu.Unlock()

	started := time.Now()
	result, scanned, err := executeQuery(ctx, request)
	finished := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if cancel := s.jobs[request.QueryExecutionID]; cancel != nil {
		cancel()
	}
	delete(s.jobs, request.QueryExecutionID)
	execution, exists := s.QueryExecutions[request.QueryExecutionID]
	if !exists {
		return
	}
	execution.Statistics.EngineExecutionTimeInMillis = finished.Sub(started).Milliseconds()
	execution.Statistics.TotalExecutionTimeInMillis = finished.Sub(execution.Status.SubmissionDateTime).Milliseconds()
	execution.Statistics.DataScannedInBytes = scanned
	if execution.Status.State != QueryExecutionStateCancelled {
		execution.Status.CompletionDateTime = &finished
		if err != nil {
			execution.Status.State = QueryExecutionStateFailed
			execution.Status.StateChangeReason = err.Error()
			execution.Status.QueryError = &QueryError{ErrorCategory: 2, ErrorMessage: err.Error()}
		} else {
			execution.Status.State = QueryExecutionStateSucceeded
			s.QueryResults[request.QueryExecutionID] = result
		}
	}
	s.saveLocked()
}

// Callers cannot race the worker by retaining pointers into stored status/config.
func cloneExecution(execution *QueryExecution) *QueryExecution {
	data, _ := json.Marshal(execution)
	var copied QueryExecution
	_ = json.Unmarshal(data, &copied)
	return &copied
}
