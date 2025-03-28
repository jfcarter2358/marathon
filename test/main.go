package main

import (
	"log/slog"
	"time"

	"github.com/jfcarter2358/marathon"
)

func emitFunc(stepName, runID, status, output string) error {
	// change this function to send state to wherever you want, e.g. a database of some kind
	slog.Info("Emitting state", "step", stepName, "runID", runID, "status", status, "output", output)
	return nil
}

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	// workflow definition
	workflow := marathon.Workflow{
		Name: "foobar",
		Steps: []marathon.Step{
			{
				Name: "a",
				Run: `name := GetContext("name")
fmt.Printf("Hello, %s!\n", name)
SetContext("foo", "bar")`,
				Success: []string{
					"b",
				},
				LogLevel: "DEBUG",
			},
			{
				Name: "b",
				Run: `fmt.Println("This is step b")
val := GetContext("foo")
panic(fmt.Errorf("this is a dummy error with context value %s", val))`,
				Error: []string{
					"c",
				},
				Always: []string{
					"d",
				},
			},
			{
				Name: "c",
				Run:  `fmt.Println("This should be run after the previous error in step b")`,
			},
			{
				Name: "d",
				Run:  `fmt.Println("This should also be run after the error in the b step")`,
			},
		},
	}

	// not yet used
	resourceTypes := make([]marathon.ResourceType, 0)
	resources := make([]marathon.Resource, 0)

	// setup marathon
	if err := marathon.Init(workflow, resources, resourceTypes); err != nil {
		slog.Error("Could not setup workflow", "error", err)
		return
	}

	// values to be passed into the run's context
	payload := map[string]any{
		"name": "Foobar",
	}

	marathon.Trigger("a", payload, emitFunc)

	// keep running so that the execution is able to finish
	for {
		time.Sleep(10 * time.Second)
	}
}
