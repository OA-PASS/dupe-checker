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

// Traverses PASS repository resources by following LDP containment relationships.  Retrieval of repository resources
// occurs in parallel.  Callers are expected to launch a ConcurrentVisitor in a background goroutine, and read accepted resources
// and errors off of channels in separate goroutines.
package visit

import (
	"dupe-checker/env"
	"dupe-checker/model"
	"dupe-checker/retrieve"
	"fmt"
	"log"
	"strings"
	"sync"
)

type ContainerHandler func(model.LdpContainer)

type EventHandler func(Event)

type ErrorHandler func(error)

// A handler implementation which does nothing with its argument
var NoopEventHandler = func(e Event) {}

// A handler implementation which does nothing with its argument
var NoopErrorHandler = func(e error) {}

// A handler implementation which does nothing with its argument
var NoopContainerHandler = func(c model.LdpContainer) {}

// A handler implementation which uses the log package to output a string representation of its argument
var LogEventHandler = func(e Event) { log.Printf("Target: %s Type: %d Message: %s", e.Target, e.EventType, e.Message) }

// A handler implementation which uses the log package to output a string representation of its argument
var LogErrorHandler = func(e error) { log.Printf("%s", e.Error()) }

// A handler implementation which uses the log package to output a string representation of its argument
var LogContainerHandler = func(c model.LdpContainer) { log.Printf("URI: %s PASS Type: %s", c.Uri(), c.PassType()) }

type ConcurrentVisitor struct {
	// retrieves LDP containers; invocation is gated by the semaphore
	retriever retrieve.Retriever
	// gates the maximum number of requests which may be performed in parallel
	semaphore chan int
	// Resources which are accepted are written to this channel
	Containers chan model.LdpContainer
	// Any errors encountered when traversing the repository are written to this channel
	Errors chan error
	// Events recording the start and end of traversing resources are written to this channel
	Events chan Event
}

var environment = env.New()

// A filter which will not descend into PASS resources (i.e. returns 'false' when the 'container' is determined to be a
// PASS resource).
var IsPassResource = func(container model.LdpContainer) bool {
	// if the container is a PASS resource, don't descend (there are no contained resources)
	// TODO double check re files
	if ok, _ := container.IsPassResource(); ok {
		return true
	}

	return false
}

// A filter which will accept PASS resources (i.e. returns 'true' when the 'container' is determined to be a PASS
// resource).
var AcceptPassResource = func(container model.LdpContainer) bool {
	return IsPassResource(container)
}

// A filter which will not descend into ACL resources (i.e. returns 'false' when the 'container' is determined to be an
// ACL).
var IsAclResource = func(container model.LdpContainer) bool {
	if strings.HasPrefix(container.Uri(), fmt.Sprintf("%s/acls", environment.FcrepoBaseUri)) || strings.Contains(container.Uri(), ".acl") {
		return true
	}

	return false
}

// Descends into every Container (hard-coded to return 'true')
var AcceptAllFilter = func(c model.LdpContainer) bool { return true }

// Accepts every PASS resource as long as the container has a non-zero URI.
var AcceptAllPassResourcesWithUris = func(c model.LdpContainer) bool {
	if isPass, _ := c.IsPassResource(); isPass {
		if len(c.Uri()) > 0 {
			return true
		}
	}
	return false
}

// Constructs a new ConcurrentVisitor instance using the supplied Retriever.  At most maxConcurrent requests are performed in
// parallel.
func New(retriever retrieve.Retriever, maxConcurrent int) ConcurrentVisitor {
	return ConcurrentVisitor{
		retriever: retriever,
		semaphore: make(chan int, maxConcurrent),
		// TODO rethink what is exported Note that Controller's Begin(...) method will overwrite these values with a new channel
		Containers: make(chan model.LdpContainer),
		// TODO rethink what is exported Note that Controller's Begin(...) method will overwrite these values with a new channel
		Errors: make(chan error),
		// TODO rethink what is exported Note that Controller's Begin(...) method will overwrite these values with a new channel
		Events: make(chan Event),
	}
}

// Given a starting URI, test each contained resource for recursion using the supplied filter.  Recurse into filtered
// resources and test each resource for acceptance.  Accepted resources will be written to the Containers channel.  Note
// the resource provided by the starting URI is tested for acceptance.
//
// This function blocks until all messages have been read off of the Errors and Containers channel.  Typically
// Walk should be invoked within a goroutine while the Errors and Containers channel are read in separate goroutines.
//
// Both filter and accept may be nil, in which case all resources are filtered for recursion, and all PASS resources are
// accepted by the Containers channel.
func (v ConcurrentVisitor) Walk(startUri string, filter, accept func(container model.LdpContainer) bool) {
	var c model.LdpContainer
	var e error

	if c, e = v.retriever.Get(startUri); e != nil {
		log.Fatalf("visit: error retrieving %s: %s", startUri, e.Error())
		return
	}

	if c.Uri() == "" {
		log.Fatalf("visit: missing container for %s", startUri)
		return
	}

	if filter == nil {
		filter = AcceptAllFilter
	}

	if accept == nil {
		accept = AcceptAllPassResourcesWithUris
	}

	if accept(c) {
		v.Containers <- c
	}

	v.walkInternal(c, filter, accept)

	close(v.Containers)
	close(v.Errors)
	close(v.Events)
}

func (v ConcurrentVisitor) walkInternal(c model.LdpContainer, filter, accept func(container model.LdpContainer) bool) {
	var e error

	// The WaitGroup insures that walkInternal(...) blocks until all the children of the supplied container have been
	// visited.  This allows for the calling method to close the channels without risking a panic (sending on a closed
	// channel)
	//
	// It also insures that when an EventDescendEndContainer is observed, the observer is guaranteed that all of the
	// container's children have been visited.
	wg := sync.WaitGroup{}
	wg.Add(len(c.Contains()))

	v.Events <- Event{c.Uri(), EventDescendStartContainer, fmt.Sprintf("STARTCONTAINER: %s", c.Uri())}
	for _, uri := range c.Contains() {
		v.semaphore <- 1
		v.Events <- Event{uri, EventDescendStart, fmt.Sprintf("START: %s", uri)}
		go func(uri string) {
			//log.Printf("visit: retrieving %s", uri)
			if c, e = v.retriever.Get(uri); e != nil {
				<-v.semaphore
				v.Errors <- fmt.Errorf("%v", VisitErr{
					Uri:     uri,
					Message: e.Error(),
					Wrapped: e,
				})
				v.Events <- Event{uri, EventDescendEnd, fmt.Sprintf("END: %s", uri)}
			} else {
				<-v.semaphore
				if accept(c) {
					v.Containers <- c
				}
				if filter(c) {
					v.walkInternal(c, filter, accept)
				}
				v.Events <- Event{uri, EventDescendEnd, fmt.Sprintf("END: %s", uri)}
			}
			wg.Done()
		}(uri)
	}
	wg.Wait()
	v.Events <- Event{c.Uri(), EventDescendEndContainer, fmt.Sprintf("ENDCONTAINER: %s", c.Uri())}
}

// Presents a facade of ConcurrentVisitor, insuring that the necessary channels and other concurrency-related state is
// managed appropriately (e.g. sync.WaitGroup and channel communication).
//
// Once initialized by NewController, an instance of Controller may be re-used; that is, Begin may be invoked multiple
// times.  The Controller is not safe for for concurrent use; it may only be invoked by one goroutine at a time.
type Controller struct {
	wg              sync.WaitGroup
	errorReader     ErrorHandler
	eventReader     EventHandler
	containerReader ContainerHandler
	visitor         ConcurrentVisitor
}

// Initializes a Controller with the supplied retriever, allowing at most maxConcurrentRequests to be executed at any
// given time.  Once initialized by NewController, an instance of Controller may be re-used; that is, Begin may be
// invoked multiple times.  The Controller is not safe for for concurrent use; it may only be invoked by one
// goroutine at a time.
//
// After initialization, the Controller *must* have an ErrorHandler, EventHandler, and ContainerHandler set, even if
// the implementation of those types are minimal.  This is because the underlying channels must be drained in order for
// the visitor to not block up.
func NewController(r retrieve.Retriever, maxConcurrentRequests int) Controller {
	c := Controller{
		visitor: New(r, maxConcurrentRequests),
	}
	return c
}

// Sets the ErrorHandler which is responsible for executing logic associated with errors emitted by the Visitor.
func (cr *Controller) ErrorHandler(handler ErrorHandler) {
	if cr.errorReader != nil {
		// TODO: would be better to provide a minimal, noop impl by default, and allow the client to set their own
		//   handler if desired.
		panic("illegal state: existing error handler")
	}
	cr.errorReader = handler
}

// Sets the EventHandler which is responsible for executing logic associated with each event emitted by the Visitor.
func (cr *Controller) EventHandler(handler EventHandler) {
	if cr.eventReader != nil {
		// TODO: would be better to provide a minimal, noop impl by default, and allow the client to set their own
		//   handler if desired.
		panic("illegal state: existing event handler")
	}
	cr.eventReader = handler
}

// Sets the ContainerHandler which is responsible for processing each model.Container that is accepted by the Visitor.
func (cr *Controller) ContainerHandler(handler ContainerHandler) {
	if cr.containerReader != nil {
		// TODO: would be better to provide a minimal, noop impl by default, and allow the client to set their own
		//   handler if desired.
		panic("illegal state: existing container handler")
	}
	cr.containerReader = handler
}

// Start walking the repository from the supplied URI.  The acceptFn determines if the visitor sends the container to
// the ContainerHandler; accepted (i.e. acceptFn returns true) containers will be routed to the ContainerHandler.  The
// filterFn determines if the visitor descends into the provided container.  If it returns 'false', the visitor will
// not descend into the container or process its children.
//
// This method blocks until all resources have been visited and all channels read.  Therefore, it is important that
// a minimal handler be set for containers, errors, and events, if only to accept the object from the channel.
//
// The Controller may be re-used; that is, Begin may be invoked multiple times.  The Controller is not safe for for
// concurrent use; it may only be invoked by one goroutine at a time.
func (cr *Controller) Begin(startingUri string, acceptFn func(container model.LdpContainer) bool, filterFn func(container model.LdpContainer) bool) {
	// FIXME if we wish to make Begin and the underlying visitor re-usable, need to rethink what is exported.
	cr.visitor.Containers = make(chan model.LdpContainer)
	cr.visitor.Errors = make(chan error)
	cr.visitor.Events = make(chan Event)

	if cr.errorReader != nil {
		cr.wg.Add(1)
		go func() {
			for err := range cr.visitor.Errors {
				cr.errorReader(err)
			}
			cr.wg.Done()
		}()
	}

	if cr.eventReader != nil {
		cr.wg.Add(1)
		go func() {
			for event := range cr.visitor.Events {
				cr.eventReader(event)
			}

			cr.wg.Done()
		}()
	}

	if cr.containerReader != nil {
		cr.wg.Add(1)
		go func() {
			for container := range cr.visitor.Containers {
				cr.containerReader(container)
			}

			cr.wg.Done()
		}()
	}

	cr.wg.Add(1)
	go func() {
		cr.visitor.Walk(startingUri, filterFn, acceptFn)
		cr.wg.Done()
	}()

	cr.wg.Wait()
}
