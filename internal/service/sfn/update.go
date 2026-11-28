package sfn

import (
	"context"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// UpdateStateMachineRequest changes the definition used by subsequent starts;
// executions already running retain the definition captured at StartExecution.
type UpdateStateMachineRequest struct {
	StateMachineArn      string                `json:"stateMachineArn"`
	Definition           string                `json:"definition,omitempty"`
	RoleArn              string                `json:"roleArn,omitempty"`
	LoggingConfiguration *LoggingConfiguration `json:"loggingConfiguration,omitempty"`
	TracingConfiguration *TracingConfiguration `json:"tracingConfiguration,omitempty"`
}

type UpdateStateMachineResponse struct {
	UpdateDate float64 `json:"updateDate"`
	RevisionID string  `json:"revisionId"`
}

// UpdateStateMachine uses the ordinary provisioning API instead of requiring
// callers to delete a state machine and its execution identity on each deploy.
func (s *Service) UpdateStateMachine(w http.ResponseWriter, r *http.Request) {
	var req UpdateStateMachineRequest
	if !decodeSFNRequest(w, r, &req) {
		return
	}
	response, err := s.storage.UpdateStateMachine(r.Context(), &req)
	if err != nil {
		handleError(w, err)
		return
	}
	writeResponse(w, response)
}

func (s *MemoryStorage) UpdateStateMachine(_ context.Context, req *UpdateStateMachineRequest) (*UpdateStateMachineResponse, error) {
	if req.Definition != "" {
		if _, err := parseDefinition(req.Definition); err != nil {
			return nil, &ServiceError{Code: errInvalidDefinition, Message: err.Error()}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sm, exists := s.StateMachines[req.StateMachineArn]
	if !exists {
		return nil, &ServiceError{Code: errStateMachineDoesNotExist, Message: "State machine does not exist"}
	}
	// Describe/List callers may still hold the prior snapshot after unlocking.
	// Replace the stored object rather than mutating those readers' fields.
	updated := *sm
	sm = &updated
	if req.Definition != "" {
		sm.Definition = req.Definition
	}
	if req.RoleArn != "" {
		sm.RoleArn = req.RoleArn
	}
	if req.LoggingConfiguration != nil {
		sm.LoggingConfiguration = req.LoggingConfiguration
	}
	if req.TracingConfiguration != nil {
		sm.TracingConfiguration = req.TracingConfiguration
	}
	sm.RevisionID = uuid.New().String()
	s.StateMachines[req.StateMachineArn] = sm
	s.saveLocked()
	return &UpdateStateMachineResponse{UpdateDate: float64(time.Now().UnixMilli()) / 1000, RevisionID: sm.RevisionID}, nil
}
