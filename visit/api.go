package visit

import (
	"dupe-checker/model"
	"fmt"
)

const (
	EventDescendStart = iota
	EventDescendEnd
	EventDescendStartContainer
	EventDescendEndContainer
	EventProcessedForDupes
)

type Event struct {
	Target    string
	EventType int
	Message   string
}

type Visitor interface {
	Walk(startUri string, filter, accept func(container model.LdpContainer) bool)
}

type VisitErr struct {
	Uri     string
	Message string
	Wrapped error
}

func (ve VisitErr) Error() string {
	return fmt.Sprintf("visit: error visiting uri %s, %s", ve.Uri, ve.Message)
}

func (ve VisitErr) Unwrap() error {
	return ve.Wrapped
}
