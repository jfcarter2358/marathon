package marathon

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	_ "embed"
)

var logLevels = map[string]string{
	"INFO":  "slog.LevelInfo",
	"DEBUG": "slog.LevelDebug",
	"WARN":  "slog.LevelWarn",
	"ERROR": "slog.LevelError",
}

// type definitions
type Step struct {
	Run      string            `json:"run" yaml:"run"`
	Name     string            `json:"name" yaml:"name"`
	Success  []string          `json:"success" yaml:"success"`
	Error    []string          `json:"error" yaml:"error"`
	Always   []string          `json:"always" yaml:"always"`
	Parents  map[string]string `json:"parent_status"`
	Inputs   []string          `json:"inputs" bson:"inputs"`
	Outputs  []string          `json:"outputs" bson:"outputs"`
	LogLevel string            `json:"log_level" bson:"log_level"`
}

type Workflow struct {
	Name            string `json:"name" yaml:"name"`
	Steps           []Step `json:"steps" yaml:"steps"`
	stepMap         map[string]Step
	ResourceTypes   []ResourceType `json:"resource_types" yaml:"resource_types"`
	resourceTypeMap map[string]ResourceType
	Resources       []Resource `json:"resources" yaml:"resources"`
	resourceMap     map[string]Resource
}

type ResourceType struct {
	Name         string `json:"name" yaml:"name"`
	Source       string `json:"source" yaml:"source"`
	Language     string `json:"language" yaml:"language"`
	Requirements string `json:"requirements" yaml:"requirements"`
}

type Resource struct {
	Name string            `json:"name" yaml:"name"`
	Args map[string]string `json:"args" yaml:"args"`
	Kind string            `json:"kind" yaml:"kind"`
}

type State struct {
	Step     string `json:"step"`
	Status   string `json:"status"`
	Output   string `json:"output"`
	Started  string `json:"started"`
	Finished string `json:"finished"`
	RunID    string `json:"run_id"`
}

// we need to create a SyncBuf in order to properly get the output of our shell commands
// I'll have to find the stack overflow post at some point that details why we need to do this
type SyncBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *SyncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *SyncBuf) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buf.Reset()
}

func (s *SyncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// run the actual step
func (s *Step) Execute(runID, dirPath string, emitFunc func(string, string, string, string) error) (State, error) {
	// status is now running
	Statuses.Set(s.Name, STATE_STATUS_RUNNING)

	state := State{
		Step:    s.Name,
		RunID:   runID,
		Status:  STATE_STATUS_RUNNING,
		Started: time.Now().String(),
	}

	// marshal our context so we can pass it in via an env variable
	contextJSON := []byte("{}")
	if v, ok := Context.mp[runID]; ok {
		var err error
		contextJSON, err = json.Marshal(v)
		if err != nil {
			slog.Error("Could not marshal context map", "error", err, "context", Context.mp)
			return state, err
		}
	}

	// TODO: Create and use a run directory emitting the status code on exit
	shell := exec.Command(
		"/bin/bash",
		"-c",
		fmt.Sprintf(`%s/step; echo "MARATHON_STATUS_CODE|MARATHON_SEP|$?"`, dirPath),
	)
	shell.Env = os.Environ()
	// set out context variable
	shell.Env = append(shell.Env, fmt.Sprintf("MARATHON_CONTEXT=%s", string(contextJSON)))

	// setup buffers
	stdout := SyncBuf{}
	shell.Stdout = &stdout
	shell.Stderr = &stdout

	shell.Start()

	contextToAdd := map[string]string{}

	for {
		// watch stdout
		if len(stdout.String()) > 0 {
			// state.Output += stdout.String()
			lines := strings.Split(stdout.String(), "\n")
			exited := false
			for _, line := range lines {
				if strings.HasPrefix(line, "MARATHON_STATUS_CODE") {
					// got a status code so the step exited
					parts := strings.Split(line, "|MARATHON_SEP|")

					state.Status = STATE_STATUS_ERROR
					if parts[1] == "0" {
						state.Status = STATE_STATUS_SUCCESS
					}

					exited = true
				} else if strings.HasPrefix(line, "MARATHON_CONTEXT_SET") {
					// a context value has been set so we should update our context
					parts := strings.Split(line, "|MARATHON_SEP|")

					contextToAdd[parts[1]] = parts[2]
				} else {
					// otherwise log to output
					state.Output += fmt.Sprintf("%s\n", line)
				}
			}
			// update our state according to the passed in emission function
			state.Update(emitFunc)
			if exited {
				break
			}
			stdout.Reset()
		}
	}
	state.Finished = time.Now().String()

	// update our run's context with the values from the step
	for k, v := range contextToAdd {
		Context.Set(runID, k, v)
	}

	// run is done
	return state, nil
}

// Update the state according to the provided emission function
func (s *State) Update(emitFunc EmitFunc) {
	if err := emitFunc(s.Step, s.RunID, s.Status, s.Output); err != nil {
		slog.Error("Could not execute emit function", "step", s.Step, "runID", s.RunID, "status", s.Status, "output", s.Output)
	}
	slog.Debug("Updated state", "step", s.Step, "runID", s.RunID, "status", s.Status, "output", s.Output)
	Statuses.Set(s.Step, s.Status)
}

// trigger the workflow according to a specific step
func (w *Workflow) Trigger(runID, stepName string, payload map[string]interface{}, emitFunc EmitFunc) error {
	// check that the step provided is actually a step
	step, ok := w.stepMap[stepName]
	if !ok {
		slog.Error("Step does not exist in workflow", "step", stepName)
		return fmt.Errorf("step %s does not exist in workflow", stepName)
	}
	// check the parents of the step to ensure they are in the correct state
	// TODO: Update this to allow for required states which we wait for, and other states which can trigger no matter the parent states
	for parent, expectedStatus := range step.Parents {
		parentStatus := Statuses.Get(parent)
		if expectedStatus == TRIGGER_STATUS_ALWAYS {
			if !Contains([]string{STATE_STATUS_SUCCESS, STATE_STATUS_ERROR}, parentStatus) {
				slog.Debug("Parent is not in correct state to trigger step", "step", stepName, "parent", parent, "parentStatus", parentStatus, "expectedStatus", expectedStatus)
				return nil
			}
			continue
		}
		if parentStatus != expectedStatus {
			slog.Debug("Parent is not in correct state to trigger step", "step", stepName, "parent", parent, "parentStatus", parentStatus, "expectedStatus", expectedStatus)
			return nil
		}
	}
	// set the status to not started
	Statuses.Set(stepName, STATE_STATUS_NOT_STARTED)

	dirPath := fmt.Sprintf("%s/%s", MARATHON_DIRECTORY, step.Name)

	// set out context according to the provided payload
	for k, v := range payload {
		Context.Set(runID, k, fmt.Sprintf("%v", v))
	}

	// execute the step
	state, err := step.Execute(runID, dirPath, emitFunc)
	if err != nil {
		slog.Error("Step failed to execute", "name", step.Name, "error", err)
		return err
	}

	slog.Debug("Step exited", "name", step.Name, "status", state.Status)

	// update the status
	Statuses.Set(step.Name, state.Status)

	// trigger any children
	w.setChildStatus(step)
	w.autoTrigger(runID, step, state, emitFunc)

	return nil
}

// check children and trigger them as appropriate
func (w *Workflow) autoTrigger(runID string, step Step, state State, emitFunc EmitFunc) error {
	switch state.Status {
	case STATE_STATUS_SUCCESS:
		// trigger on success
		for _, name := range step.Success {
			go w.Trigger(runID, name, map[string]interface{}{}, emitFunc)
		}
		for _, name := range step.Always {
			go w.Trigger(runID, name, map[string]interface{}{}, emitFunc)
		}
	case STATE_STATUS_ERROR:
		// trigger on error
		for _, name := range step.Error {
			go w.Trigger(runID, name, map[string]interface{}{}, emitFunc)
		}
		for _, name := range step.Always {
			go w.Trigger(runID, name, map[string]interface{}{}, emitFunc)
		}
	default:
		// unknown status of the step which is not expected
		slog.Error("Unknown status in auto trigger", "step", step.Name, "status", state.Status)
		return fmt.Errorf("unknown status in auto trigger")
	}
	return nil
}

// setup the workflow
func (w *Workflow) Setup() error {
	// go through our steps and calculate their parents
	for _, step := range w.Steps {
		if step.Parents == nil {
			step.Parents = make(map[string]string)
		}
		Statuses.Set(step.Name, STATE_STATUS_NOT_STARTED)
		for _, otherStep := range w.Steps {
			if Contains(otherStep.Success, step.Name) {
				step.Parents[otherStep.Name] = TRIGGER_STATUS_SUCCESS
				continue
			}
			if Contains(otherStep.Error, step.Name) {
				step.Parents[otherStep.Name] = TRIGGER_STATUS_ERROR
				continue
			}
			if Contains(otherStep.Always, step.Name) {
				step.Parents[otherStep.Name] = TRIGGER_STATUS_ALWAYS
			}
		}
		// add the step to a map based off the name for faster access during execution
		w.stepMap[step.Name] = step
	}

	for name, step := range w.stepMap {
		// Setup the path in the file structure
		dirPath := fmt.Sprintf("%s/%s", MARATHON_DIRECTORY, name)
		if err := os.MkdirAll(dirPath, 0755); err != nil {
			slog.Error("Could not create step directory", "name", name, "error", err, "path", dirPath)
			return err
		}

		logLevel := "slog.LevelInfo"
		if step.LogLevel != "" {
			if val, ok := logLevels[step.LogLevel]; ok {
				logLevel = val
			} else {
				slog.Error("Unknown log level", "level", step.LogLevel)
				return fmt.Errorf("unknown log level: %s", step.LogLevel)
			}
		}

		// write out the step contents
		contents := fmt.Sprintf(`package main

import (
	"encoding/json"
	"log/slog"
	"os"
)

var Context map[string]string

func GetContext(k string) string {
	if v, ok := Context[k]; ok {
		slog.Debug("Got context value", "key", k, "value", v)
		return v
	}
	slog.Debug("Could not find context value", "key", k, "context", Context)
	return ""
}

func SetContext(k, v string) {
	Context[k] = v
	slog.Debug("Set context value", "key", k, "value", v)
	fmt.Printf("MARATHON_CONTEXT_SET|MARATHON_SEP|%%s|MARATHON_SEP|%%s", k, v)
}

func main() {
	contextString := os.Getenv("MARATHON_CONTEXT")
	if err := json.Unmarshal([]byte(contextString), &Context); err != nil {
		slog.Error("Unable to unmarshal context", "context", contextString, "error", err)
		return
	}
	slog.SetLogLoggerLevel(%s)
	%s
}`, logLevel, step.Run)
		scriptPath := fmt.Sprintf("%s/main.go", dirPath)
		if err := os.WriteFile(scriptPath, []byte(contents), 0777); err != nil {
			slog.Error("Could not write out step file", "name", name, "error", err, "path", scriptPath)
			return err
		}

		// Resolve imports
		cmd := fmt.Sprintf("cd %s && goimports -w main.go", dirPath)
		out, err := executeCommand(cmd)
		if err != nil {
			slog.Error("Unable to get imports for step", "name", name, "error", err, "output", out)
			return err
		}

		// Compile the step
		cmd = fmt.Sprintf("cd %s && go build -o step", dirPath)
		out, err = executeCommand(cmd)
		if err != nil {
			slog.Error("Unable to compile step", "name", name, "error", err, "output", out)
			return err
		}
	}

	return nil
}

// execute a shell command and get the output
func executeCommand(cmd string) (string, error) {
	out, err := exec.Command(
		"/bin/bash",
		"-c",
		cmd,
	).CombinedOutput()

	return string(out), err
}

// set the status of each child to not started if the parent is triggered
func (w *Workflow) setChildStatus(s Step) {
	for _, child := range s.Success {
		Statuses.Set(child, STATE_STATUS_NOT_STARTED)
		w.setChildStatus(w.stepMap[child])
	}
	for _, child := range s.Error {
		Statuses.Set(child, STATE_STATUS_NOT_STARTED)
		w.setChildStatus(w.stepMap[child])
	}
	for _, child := range s.Always {
		Statuses.Set(child, STATE_STATUS_NOT_STARTED)
		w.setChildStatus(w.stepMap[child])
	}
}
