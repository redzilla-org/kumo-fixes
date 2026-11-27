package sfn

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/sivchari/kumo/internal/storage"
)

// Error codes.
const (
	errStateMachineDoesNotExist  = "StateMachineDoesNotExist"
	errStateMachineAlreadyExists = "StateMachineAlreadyExists"
	errExecutionDoesNotExist     = "ExecutionDoesNotExist"
	errExecutionAlreadyExists    = "ExecutionAlreadyExists"
	errInvalidArn                = "InvalidArn"
	errInvalidDefinition         = "InvalidDefinition"
	// errResourceNotFound is DescribeMapRun's documented error for a
	// mapRunArn kumo has never recorded.
	errResourceNotFound = "ResourceNotFound"

	msgStateMachineDoesNotExist = "State machine does not exist"
	msgExecutionDoesNotExist    = "Execution does not exist"
)

// Storage defines the Step Functions storage interface.
type Storage interface {
	// State machine operations.
	CreateStateMachine(ctx context.Context, req *CreateStateMachineRequest) (*StateMachine, error)
	UpdateStateMachine(ctx context.Context, req *UpdateStateMachineRequest) (*UpdateStateMachineResponse, error)
	DeleteStateMachine(ctx context.Context, arn string) error
	DescribeStateMachine(ctx context.Context, arn string) (*StateMachine, error)
	ListStateMachines(ctx context.Context, maxResults int32, nextToken string) ([]*StateMachine, string, error)

	// Execution operations.
	StartExecution(ctx context.Context, stateMachineArn, name, input, traceHeader string) (*Execution, error)
	StopExecution(ctx context.Context, executionArn, errorCode, cause string) (*Execution, error)
	DescribeExecution(ctx context.Context, executionArn string) (*Execution, error)
	ListExecutions(ctx context.Context, stateMachineArn, statusFilter string, maxResults int32, nextToken string) ([]*Execution, string, error)
	GetExecutionHistory(ctx context.Context, executionArn string, maxResults int32, nextToken string, reverseOrder bool) ([]*HistoryEvent, string, error)

	// Map Run operations (see maprun.go): distributed-mode Map states each
	// create a Map Run record, queryable the same way real AWS's own
	// DescribeMapRun/ListMapRuns are.
	DescribeMapRun(ctx context.Context, mapRunArn string) (*MapRun, error)
	ListMapRuns(ctx context.Context, executionArn string, maxResults int32, nextToken string) ([]*MapRun, string, error)

	// Tag operations.
	TagResource(ctx context.Context, resourceArn string, tags []Tag) error
	UntagResource(ctx context.Context, resourceArn string, tagKeys []string) error
	ListTagsForResource(ctx context.Context, resourceArn string) ([]Tag, error)

	// Version and alias operations.
	ListStateMachineVersions(ctx context.Context, stateMachineArn string, maxResults int32, nextToken string) ([]map[string]string, string, error)
	ListStateMachineAliases(ctx context.Context, stateMachineArn string, maxResults int32, nextToken string) ([]map[string]string, string, error)

	// Activity operations.
	CreateActivity(ctx context.Context, name string, tags []Tag) (*Activity, error)
	DescribeActivity(ctx context.Context, arn string) (*Activity, error)
	ListActivities(ctx context.Context, maxResults int32, nextToken string) ([]*Activity, string, error)
	DeleteActivity(ctx context.Context, arn string) error
	GetActivityTask(ctx context.Context, activityArn, workerName string) (taskToken, input string, err error)

	// Task token operations, shared by callback (.waitForTaskToken) Task
	// states and activity Task states.
	SendTaskSuccess(ctx context.Context, taskToken, output string) error
	SendTaskFailure(ctx context.Context, taskToken, errorName, cause string) error
	SendTaskHeartbeat(ctx context.Context, taskToken string) error

	// DispatchAction dispatches the request to the appropriate handler.
	DispatchAction(action string) bool
}

// Option is a configuration option for MemoryStorage.
type Option func(*MemoryStorage)

// WithDataDir enables persistent storage in the specified directory.
func WithDataDir(dir string) Option {
	return func(s *MemoryStorage) {
		s.dataDir = dir
	}
}

// WithBaseURL sets the base URL for cross-service HTTP calls (SQS, Lambda).
func WithBaseURL(url string) Option {
	return func(s *MemoryStorage) {
		s.baseURL = url
	}
}

// Compile-time interface checks.
var (
	_ json.Marshaler   = (*MemoryStorage)(nil)
	_ json.Unmarshaler = (*MemoryStorage)(nil)
)

// MemoryStorage implements Storage with in-memory data.
type MemoryStorage struct {
	mu            sync.RWMutex              `json:"-"`
	StateMachines map[string]*StateMachine  `json:"stateMachines"`
	Executions    map[string]*ExecutionData `json:"executions"`
	Activities    map[string]*Activity      `json:"activities"`
	Tags          map[string][]Tag          `json:"tags"`
	MapRuns       map[string]*MapRun        `json:"mapRuns"`
	region        string
	accountID     string
	EventCounter  int64 `json:"eventCounter"`
	dataDir       string
	baseURL       string
	engine        *executionEngine
}

// ExecutionData holds execution information and its history.
type ExecutionData struct {
	Execution *Execution      `json:"execution"`
	History   []*HistoryEvent `json:"history"`
	// StopExecution cancels the engine after the start request has returned.
	cancel context.CancelFunc
}

// NewMemoryStorage creates a new in-memory storage.
func NewMemoryStorage(opts ...Option) *MemoryStorage {
	region := os.Getenv("AWS_DEFAULT_REGION")
	if region == "" {
		region = "us-east-1"
	}

	s := &MemoryStorage{
		StateMachines: make(map[string]*StateMachine),
		Executions:    make(map[string]*ExecutionData),
		Activities:    make(map[string]*Activity),
		Tags:          make(map[string][]Tag),
		MapRuns:       make(map[string]*MapRun),
		region:        region,
		accountID:     "000000000000",
		baseURL:       defaultBaseURL,
	}
	for _, o := range opts {
		o(s)
	}

	s.engine = newExecutionEngine(s.baseURL)
	// Wire the engine's states:startExecution and Map Run bookkeeping back to
	// this storage as a direct reference rather than an HTTP round trip.
	s.engine.starter = s
	s.engine.mapRuns = s

	if s.dataDir != "" {
		_ = storage.Load(s.dataDir, "states", s)
	}

	return s
}

// MarshalJSON serializes the storage state to JSON.
func (s *MemoryStorage) MarshalJSON() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	type Alias MemoryStorage

	data, err := json.Marshal(&struct{ *Alias }{Alias: (*Alias)(s)})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal: %w", err)
	}

	return data, nil
}

// UnmarshalJSON restores the storage state from JSON.
func (s *MemoryStorage) UnmarshalJSON(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	type Alias MemoryStorage

	aux := &struct{ *Alias }{Alias: (*Alias)(s)}

	if err := json.Unmarshal(data, aux); err != nil {
		return fmt.Errorf("failed to unmarshal: %w", err)
	}

	if s.StateMachines == nil {
		s.StateMachines = make(map[string]*StateMachine)
	}

	if s.Executions == nil {
		s.Executions = make(map[string]*ExecutionData)
	}

	if s.Activities == nil {
		s.Activities = make(map[string]*Activity)
	}

	if s.Tags == nil {
		s.Tags = make(map[string][]Tag)
	}

	if s.MapRuns == nil {
		s.MapRuns = make(map[string]*MapRun)
	}

	return nil
}

// saveLocked persists the current state to disk while the caller holds the lock.
func (s *MemoryStorage) saveLocked() {
	if s.dataDir == "" {
		return
	}

	storage.ScheduleSave(s.dataDir, "states", s.MarshalJSON)
}

// Close saves the storage state to disk if persistence is enabled.
func (s *MemoryStorage) Close() error {
	if s.dataDir == "" {
		return nil
	}

	if err := storage.Save(s.dataDir, "states", s); err != nil {
		return fmt.Errorf("failed to save: %w", err)
	}

	return nil
}

// CreateStateMachine creates a new state machine.
func (s *MemoryStorage) CreateStateMachine(_ context.Context, req *CreateStateMachineRequest) (*StateMachine, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	arn := fmt.Sprintf("arn:aws:states:%s:%s:stateMachine:%s", s.region, s.accountID, req.Name)

	if _, exists := s.StateMachines[arn]; exists {
		return nil, &ServiceError{Code: errStateMachineAlreadyExists, Message: "State machine already exists"}
	}

	smType := StateMachineTypeStandard
	if req.Type == "EXPRESS" {
		smType = StateMachineTypeExpress
	}

	now := time.Now()
	sm := &StateMachine{
		StateMachineArn:      arn,
		Name:                 req.Name,
		Definition:           req.Definition,
		RoleArn:              req.RoleArn,
		Type:                 smType,
		Status:               StateMachineStatusActive,
		CreationDate:         now,
		LoggingConfiguration: req.LoggingConfiguration,
		TracingConfiguration: req.TracingConfiguration,
		RevisionID:           uuid.New().String(),
	}

	s.StateMachines[arn] = sm

	if len(req.Tags) > 0 {
		s.Tags[arn] = append([]Tag{}, req.Tags...)
	}

	s.saveLocked()

	return sm, nil
}

// DeleteStateMachine deletes a state machine.
func (s *MemoryStorage) DeleteStateMachine(_ context.Context, arn string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.StateMachines[arn]; !exists {
		return &ServiceError{Code: errStateMachineDoesNotExist, Message: msgStateMachineDoesNotExist}
	}

	delete(s.StateMachines, arn)
	delete(s.Tags, arn)

	s.saveLocked()

	return nil
}

// DescribeStateMachine describes a state machine.
func (s *MemoryStorage) DescribeStateMachine(_ context.Context, arn string) (*StateMachine, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	sm, exists := s.StateMachines[arn]
	if !exists {
		return nil, &ServiceError{Code: errStateMachineDoesNotExist, Message: msgStateMachineDoesNotExist}
	}

	return sm, nil
}

// ListStateMachines lists all state machines.
func (s *MemoryStorage) ListStateMachines(_ context.Context, maxResults int32, _ string) ([]*StateMachine, string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if maxResults <= 0 {
		maxResults = 100
	}

	stateMachines := make([]*StateMachine, 0, len(s.StateMachines))
	for _, sm := range s.StateMachines {
		stateMachines = append(stateMachines, sm)
	}

	// Sort by creation date.
	sort.Slice(stateMachines, func(i, j int) bool {
		return stateMachines[i].CreationDate.Before(stateMachines[j].CreationDate)
	})

	if int32(len(stateMachines)) > maxResults { //nolint:gosec // slice length bounded by maxResults parameter
		stateMachines = stateMachines[:maxResults]
	}

	return stateMachines, "", nil
}

// StartExecution starts a new execution.
func (s *MemoryStorage) StartExecution(ctx context.Context, stateMachineArn, name, input, traceHeader string) (*Execution, error) {
	return s.startExecutionAtDepth(ctx, stateMachineArn, name, input, traceHeader, 0)
}

// startNestedExecution implements executionStarter for a
// states:startExecution Task: like StartExecution, but carries the caller's
// nesting depth through since StartExecution's ctx parameter cannot.
func (s *MemoryStorage) startNestedExecution(ctx context.Context, stateMachineArn, name, input string, depth int) (*Execution, error) {
	return s.startExecutionAtDepth(ctx, stateMachineArn, name, input, "", depth)
}

// startExecutionAtDepth is StartExecution/startNestedExecution's shared
// implementation.
func (s *MemoryStorage) startExecutionAtDepth(requestCtx context.Context, stateMachineArn, name, input, traceHeader string, depth int) (*Execution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sm, exists := s.StateMachines[stateMachineArn]
	if !exists {
		return nil, &ServiceError{Code: errStateMachineDoesNotExist, Message: msgStateMachineDoesNotExist}
	}

	execName := name
	if execName == "" {
		execName = uuid.New().String()
	}

	executionArn := fmt.Sprintf("arn:aws:states:%s:%s:execution:%s:%s", s.region, s.accountID, sm.Name, execName)

	if _, exists := s.Executions[executionArn]; exists {
		return nil, &ServiceError{Code: errExecutionAlreadyExists, Message: "Execution already exists"}
	}

	now := time.Now()
	exec := s.createExecution(executionArn, stateMachineArn, execName, input, traceHeader, now)

	startID := int64(1)
	history := []*HistoryEvent{
		{
			Timestamp: now, Type: HistoryEventTypeExecutionStarted, ID: startID, PreviousEventID: 0,
			ExecutionStartedEventDetails: &ExecutionStartedEventDetails{
				Input: input, InputDetails: &CloudWatchEventsExecutionDataDetails{Included: true}, RoleArn: sm.RoleArn,
			},
		},
	}

	ed := &ExecutionData{Execution: exec, History: history}
	// StartExecution is asynchronous: retain values, not the HTTP disconnect.
	ctx, cancel := context.WithCancel(context.WithoutCancel(requestCtx))
	ed.cancel = cancel
	s.Executions[executionArn] = ed

	s.saveLocked()

	// Parse the definition and run the state machine asynchronously.
	definition := sm.Definition

	go s.runExecution(ctx, ed, definition, input, startID, depth)

	return copyExecution(exec), nil
}

// runExecution obeys ASL TimeoutSeconds and explicit StopExecution, without an
// emulator-only cap that truncates otherwise valid long-running Lambda builds.
func (s *MemoryStorage) runExecution(ctx context.Context, ed *ExecutionData, definition, input string, lastEventID int64, depth int) {
	defer ed.cancel()
	def, err := parseDefinition(definition)
	if err != nil {
		s.failExecution(ed, lastEventID, errorStatesRuntime, fmt.Sprintf("Failed to parse definition: %v", err))
		return
	}
	if def.TimeoutSeconds != nil && *def.TimeoutSeconds > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(*def.TimeoutSeconds)*time.Second)
		defer cancel()
	}
	ctx = withNestedExecutionDepth(ctx, depth)
	ctx = withExecutionArn(ctx, ed.Execution.ExecutionArn)
	ctx = context.WithValue(ctx, executionContextKey{}, map[string]any{
		"Execution":    map[string]any{"Id": ed.Execution.ExecutionArn, "Name": ed.Execution.Name, "StartTime": ed.Execution.StartDate.UTC().Format(time.RFC3339Nano)},
		"StateMachine": map[string]any{"Id": ed.Execution.StateMachineArn},
	})
	ctx = context.WithValue(ctx, historyContextKey{}, &historyCursor{last: lastEventID, region: s.region, append: func(event *HistoryEvent) int64 { return s.appendHistory(ed, event) }})
	output, err := s.engine.execute(ctx, def, input)
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			s.finishExecution(ed, ExecutionStatusTimedOut, "", errorStatesTimeout, fmt.Sprintf("State machine execution exceeded its TimeoutSeconds (%d)", *def.TimeoutSeconds))
			return
		}
		s.failExecution(ed, lastEventID, executionErrorCode(err), executionErrorCause(err))
		return
	}
	s.succeedExecution(ed, lastEventID, output)
}

// succeedExecution marks an execution as SUCCEEDED.
func (s *MemoryStorage) succeedExecution(ed *ExecutionData, _ int64, output string) {
	s.finishExecution(ed, ExecutionStatusSucceeded, output, "", "")
}

// failExecution marks an execution as FAILED.
func (s *MemoryStorage) failExecution(ed *ExecutionData, _ int64, errorCode, cause string) {
	s.finishExecution(ed, ExecutionStatusFailed, "", errorCode, cause)
}

// finishExecution shares StopExecution's lock so late HTTP responses cannot
// overwrite ABORTED, and every execution owns its own sequential history IDs.
func (s *MemoryStorage) finishExecution(ed *ExecutionData, status ExecutionStatus, output, errorCode, cause string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ed.Execution.Status != ExecutionStatusRunning {
		return
	}
	now := time.Now()
	ed.Execution.Status = status
	ed.Execution.StopDate = &now
	ed.Execution.Error = errorCode
	ed.Execution.Cause = cause
	event := &HistoryEvent{Timestamp: now, ID: int64(len(ed.History)) + 1, PreviousEventID: int64(len(ed.History))}
	switch status {
	case ExecutionStatusSucceeded:
		ed.Execution.Output = output
		ed.Execution.OutputDetails = &CloudWatchEventsExecutionDataDetails{Included: true}
		event.Type = HistoryEventTypeExecutionSucceeded
		event.ExecutionSucceededEventDetails = &ExecutionSucceededEventDetails{Output: output, OutputDetails: ed.Execution.OutputDetails}
	case ExecutionStatusTimedOut:
		event.Type = HistoryEventTypeExecutionTimedOut
		event.ExecutionTimedOutEventDetails = &ExecutionTimedOutEventDetails{Error: errorCode, Cause: cause}
	default:
		event.Type = HistoryEventTypeExecutionFailed
		event.ExecutionFailedEventDetails = &ExecutionFailedEventDetails{Error: errorCode, Cause: cause}
	}
	ed.History = append(ed.History, event)
	s.saveLocked()
}

// createExecution creates a new execution object.
func (s *MemoryStorage) createExecution(arn, smArn, name, input, traceHeader string, now time.Time) *Execution {
	return &Execution{
		ExecutionArn:    arn,
		StateMachineArn: smArn,
		Name:            name,
		Status:          ExecutionStatusRunning,
		StartDate:       now,
		Input:           input,
		InputDetails:    &CloudWatchEventsExecutionDataDetails{Included: true},
		TraceHeader:     traceHeader,
	}
}

// copyExecution returns a shallow copy of an execution so callers never hold
// a pointer that the background execution goroutine keeps mutating. Writers
// only assign whole field values, so a shallow copy is race-free.
func copyExecution(e *Execution) *Execution {
	c := *e

	return &c
}

// StopExecution stops an execution.
func (s *MemoryStorage) StopExecution(_ context.Context, executionArn, errorCode, cause string) (*Execution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ed, exists := s.Executions[executionArn]
	if !exists {
		return nil, &ServiceError{Code: errExecutionDoesNotExist, Message: msgExecutionDoesNotExist}
	}

	if ed.Execution.Status != ExecutionStatusRunning {
		// Already stopped.
		return copyExecution(ed.Execution), nil
	}

	now := time.Now()
	ed.Execution.Status = ExecutionStatusAborted
	ed.Execution.StopDate = &now
	ed.Execution.Error = errorCode
	ed.Execution.Cause = cause

	// Add abort event.
	if ed.cancel != nil {
		ed.cancel()
	}
	eventID := int64(len(ed.History)) + 1
	abortEvent := &HistoryEvent{
		Timestamp:       now,
		Type:            HistoryEventTypeExecutionAborted,
		ID:              eventID,
		PreviousEventID: int64(len(ed.History)),
		ExecutionAbortedEventDetails: &ExecutionAbortedEventDetails{
			Error: errorCode,
			Cause: cause,
		},
	}

	ed.History = append(ed.History, abortEvent)

	s.saveLocked()

	return copyExecution(ed.Execution), nil
}

// DescribeExecution describes an execution.
func (s *MemoryStorage) DescribeExecution(_ context.Context, executionArn string) (*Execution, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ed, exists := s.Executions[executionArn]
	if !exists {
		return nil, &ServiceError{Code: errExecutionDoesNotExist, Message: msgExecutionDoesNotExist}
	}

	return copyExecution(ed.Execution), nil
}

// ListExecutions lists executions for a state machine.
func (s *MemoryStorage) ListExecutions(_ context.Context, stateMachineArn, statusFilter string, maxResults int32, _ string) ([]*Execution, string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if maxResults <= 0 {
		maxResults = 100
	}

	var executions []*Execution

	for _, ed := range s.Executions {
		if ed.Execution.StateMachineArn != stateMachineArn {
			continue
		}

		if statusFilter != "" && string(ed.Execution.Status) != statusFilter {
			continue
		}

		executions = append(executions, copyExecution(ed.Execution))
	}

	// Sort by start date (most recent first).
	sort.Slice(executions, func(i, j int) bool {
		return executions[i].StartDate.After(executions[j].StartDate)
	})

	if int32(len(executions)) > maxResults { //nolint:gosec // slice length bounded by maxResults parameter
		executions = executions[:maxResults]
	}

	return executions, "", nil
}

// DescribeMapRun describes a Map Run (see maprun.go).
func (s *MemoryStorage) DescribeMapRun(_ context.Context, mapRunArn string) (*MapRun, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	mr, exists := s.MapRuns[mapRunArn]
	if !exists {
		return nil, &ServiceError{Code: errResourceNotFound, Message: "Map Run does not exist"}
	}

	return copyMapRun(mr), nil
}

// ListMapRuns lists the Map Runs started by a given execution.
func (s *MemoryStorage) ListMapRuns(_ context.Context, executionArn string, maxResults int32, _ string) ([]*MapRun, string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, exists := s.Executions[executionArn]; !exists {
		return nil, "", &ServiceError{Code: errExecutionDoesNotExist, Message: msgExecutionDoesNotExist}
	}

	if maxResults <= 0 {
		maxResults = 100
	}

	var mapRuns []*MapRun

	for _, mr := range s.MapRuns {
		if mr.ExecutionArn != executionArn {
			continue
		}

		mapRuns = append(mapRuns, copyMapRun(mr))
	}

	sort.Slice(mapRuns, func(i, j int) bool {
		return mapRuns[i].StartDate.Before(mapRuns[j].StartDate)
	})

	if len(mapRuns) > int(maxResults) {
		mapRuns = mapRuns[:int(maxResults)]
	}

	return mapRuns, "", nil
}

// GetExecutionHistory paginates a stable snapshot, including while running.
func (s *MemoryStorage) GetExecutionHistory(_ context.Context, executionArn string, maxResults int32, nextToken string, reverseOrder bool) ([]*HistoryEvent, string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ed, exists := s.Executions[executionArn]
	if !exists {
		return nil, "", &ServiceError{Code: errExecutionDoesNotExist, Message: msgExecutionDoesNotExist}
	}
	if maxResults <= 0 {
		maxResults = 100
	}
	page := historyPage{Execution: executionArn, Size: len(ed.History), Reverse: reverseOrder}
	if nextToken != "" {
		encoded, err := base64.RawURLEncoding.DecodeString(nextToken)
		if err != nil || json.Unmarshal(encoded, &page) != nil || page.Execution != executionArn || page.Reverse != reverseOrder || page.Size < 0 || page.Size > len(ed.History) || page.Offset < 0 || page.Offset > page.Size {
			return nil, "", &ServiceError{Code: "InvalidToken", Message: "Invalid execution history token"}
		}
	}
	events := append([]*HistoryEvent{}, ed.History[:page.Size]...)
	if reverseOrder {
		for i, j := 0, len(events)-1; i < j; i, j = i+1, j-1 {
			events[i], events[j] = events[j], events[i]
		}
	}
	end := min(page.Offset+int(maxResults), len(events))
	result := events[page.Offset:end]
	if end == len(events) {
		return result, "", nil
	}
	page.Offset = end
	return result, historyPageToken(page), nil
}

// TagResource adds tags to a resource.
func (s *MemoryStorage) TagResource(_ context.Context, resourceArn string, tags []Tag) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	existingTags := s.Tags[resourceArn]
	tagMap := make(map[string]string)

	for _, tag := range existingTags {
		tagMap[tag.Key] = tag.Value
	}

	for _, tag := range tags {
		tagMap[tag.Key] = tag.Value
	}

	newTags := make([]Tag, 0, len(tagMap))

	for k, v := range tagMap {
		newTags = append(newTags, Tag{Key: k, Value: v})
	}

	s.Tags[resourceArn] = newTags

	s.saveLocked()

	return nil
}

// UntagResource removes tags from a resource.
func (s *MemoryStorage) UntagResource(_ context.Context, resourceArn string, tagKeys []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	existingTags := s.Tags[resourceArn]
	keySet := make(map[string]bool)

	for _, key := range tagKeys {
		keySet[key] = true
	}

	newTags := make([]Tag, 0)

	for _, tag := range existingTags {
		if !keySet[tag.Key] {
			newTags = append(newTags, tag)
		}
	}

	s.Tags[resourceArn] = newTags

	s.saveLocked()

	return nil
}

// ListTagsForResource lists tags for a resource.
func (s *MemoryStorage) ListTagsForResource(_ context.Context, resourceArn string) ([]Tag, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tags := s.Tags[resourceArn]
	if tags == nil {
		tags = make([]Tag, 0)
	}

	return tags, nil
}

// ListStateMachineVersions lists versions for a state machine.
// Versions are not modeled in kumo; this always returns an empty list.
func (s *MemoryStorage) ListStateMachineVersions(_ context.Context, _ string, _ int32, _ string) ([]map[string]string, string, error) {
	return []map[string]string{}, "", nil
}

// ListStateMachineAliases lists aliases for a state machine.
// Aliases are not modeled in kumo; this always returns an empty list.
func (s *MemoryStorage) ListStateMachineAliases(_ context.Context, _ string, _ int32, _ string) ([]map[string]string, string, error) {
	return []map[string]string{}, "", nil
}

// DispatchAction checks if the action is valid.
func (s *MemoryStorage) DispatchAction(_ string) bool {
	return true
}
