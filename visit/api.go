//
// Copyright 2021 Johns Hopkins University
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

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
	EventProcessedContainer
)

type Event struct {
	Target    string
	EventType int
	Message   string
}

type Visitor interface {
	// Perform a depth-first traversal of LDP resources beginning with the 'startUri'.  The 'filter' and 'accept'
	// functions act as follows:
	//
	// If the 'filter' function returns 'true', then Visitor will descend into the container supplied to the function.
	//
	// If the 'accept' function returns 'true', then Visitor will accept that container for processing.
	//
	// The 'filter' function is useful for skipping containers that will rarely need any kind of processing (such as
	// Fedora's .acl resources), or for containers which have already had domain processing applied.
	//
	// Depending on the implementation, the 'accept' function is intended to apply the caller's business logic.
	// ConcurrentVisitor's implementation of Walk uses 'accept' to determine whether or not the container should be
	// sent to a channel, which is read by the caller.
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
