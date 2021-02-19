// Traverses PASS repository resources by following LDP containment relationships.  Retrieval of repository resources
// occurs in parallel.  Callers are expected to launch a ConcurrentVisitor in a background goroutine, and read accepted resources
// and errors off of channels in separate goroutines.
package visit

import (
	"dupe-checker/model"
	"dupe-checker/retriever"
	"fmt"
	"log"
	"sync"
)

type ConcurrentVisitor struct {
	// retrieves LDP containers; invocation is gated by the semaphore
	retriever retriever.Retriever
	// gates the maximum number of requests which may be performed in parallel
	semaphore chan int
	// Resources which are accepted are written to this channel
	Containers chan model.LdpContainer
	// Any errors encountered when traversing the repository are written to this channel
	Errors chan error
	// Events recording the start and end of traversing resources are written to this channel
	Events chan Event
}

// descends into every container
var defaultFilter = func(c model.LdpContainer) bool { return true }

// accepts every PASS resource
var defaultAccept = func(c model.LdpContainer) bool {
	if isPass, _ := c.IsPassResource(); isPass {
		if len(c.Uri()) > 0 {
			return true
		}
	}
	return false
}

// Constructs a new ConcurrentVisitor instance using the supplied Retriever.  At most maxConcurrent requests are performed in
// parallel.
func New(retriever retriever.Retriever, maxConcurrent int) ConcurrentVisitor {
	return ConcurrentVisitor{
		retriever:  retriever,
		semaphore:  make(chan int, maxConcurrent),
		Containers: make(chan model.LdpContainer),
		Errors:     make(chan error),
		Events:     make(chan Event),
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
// accepted.
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
		filter = defaultFilter
	}

	if accept == nil {
		accept = defaultAccept
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
			log.Printf("visit: retrieving %s", uri)
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
