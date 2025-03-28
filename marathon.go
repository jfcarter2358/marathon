package marathon

import (
	"log/slog"
	"sync"

	"github.com/google/uuid"
)

// Use SyncMap to safely access a package-level map from our goroutines
type SyncMap struct {
	mu sync.Mutex
	mp map[string]string
}

func (s *SyncMap) Init() {
	s.mp = make(map[string]string)
}

func (s *SyncMap) Set(k, v string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	slog.Debug("Setting SyncMap value", "key", k, "value", v)
	s.mp[k] = v
}

func (s *SyncMap) Get(k string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if val, ok := s.mp[k]; ok {
		return val
	}
	return ""
}

// Use SyncContext to safely access a package-level context map from our goroutines
type SyncContext struct {
	mu sync.Mutex
	mp map[string]map[string]string
}

func (s *SyncContext) Init() {
	s.mp = make(map[string]map[string]string)
}

func (s *SyncContext) Set(r, k, v string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	slog.Debug("Setting SyncContext value", "run", r, "key", k, "value", v)
	if s.mp == nil {
		s.mp = make(map[string]map[string]string)
	}
	if _, ok := s.mp[r]; !ok {
		s.mp[r] = make(map[string]string)
	}
	s.mp[r][k] = v
}

func (s *SyncContext) Get(r, k string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if m, ok := s.mp[r]; ok {
		if val, ok := m[k]; ok {
			return val
		}
	}
	return ""
}

// Create EmitFunc type for readability in the code base
type EmitFunc func(string, string, string, string) error

var Statuses SyncMap
var Context SyncContext
var workflow Workflow

// not used at the moment, might clean up later if we don't end up using them
// var resources []Resource
// var resourceTypes []ResourceType

func Init(w Workflow, rs []Resource, rts []ResourceType) error {
	workflow = w
	// resources = rs
	// resourceTypes = rts

	// initialize variables
	Statuses.Init()
	workflow.stepMap = make(map[string]Step)

	// setup the workflow steps on the file system
	if err := workflow.Setup(); err != nil {
		slog.Error("Failed to setup workflow", "name", workflow.Name, "error", err)
		return err
	}

	slog.Info("Successfully setup workflow", "name", workflow.Name)
	return nil
}

// create a run and execute the workflow
func Trigger(stepName string, payload map[string]interface{}, emitFunc EmitFunc) {
	runID := uuid.NewString()
	go workflow.Trigger(runID, stepName, payload, emitFunc)
}
