package visit

import (
	"dupe-checker/model"
	"dupe-checker/retriever"
	"fmt"
	"log"
)

type Visitor struct {
	retriever  retriever.Retriever
	semaphore  chan int
	uris       chan string
	containers chan model.LdpContainer
	errors     chan error
}

type VisitErr struct {
	Uri     string
	Message string
	Wrapped error
}

const (
	BreadthFirst = iota
	DepthFirst
)

type Kind int

var defaultFilter = func(c model.LdpContainer) bool { return true }

func New(retriever retriever.Retriever) Visitor {
	return Visitor{
		retriever: retriever,
	}
}

func (ve VisitErr) Error() string {
	return fmt.Sprintf("visit: error visiting uri %s, %s", ve.Uri, ve.Message)
}

func (ve VisitErr) Unwrap() error {
	return ve.Wrapped
}

func (v Visitor) Walk(uri string) {
	// launch the visit goroutine which will block waiting for a list of uris.
	go v.visit()

	var c model.LdpContainer
	var e error

	v.semaphore <- 1
	if c, e = v.retriever.Get(uri); e != nil {
		v.errors <- fmt.Errorf("visit: error retrieving %s: %s", uri, e.Error())
		v.semaphore <- 1
		return
	}

	if c.Uri() == "" {
		v.errors <- fmt.Errorf("visit: missing container for %s", uri)
		return
	}

	v.walkInternal(c)

	return
}

func (v Visitor) walkInternal(c model.LdpContainer) {
	// Feeds a list of uris to visit(...) via the uris channel
	for _, containedUri := range c.Contains() {
		v.uris <- containedUri
	}

}

func (v Visitor) visit() {
	var e error
	var c model.LdpContainer

	for uri := range v.uris {
		v.semaphore <- 1
		go func(uri string) {
			log.Printf("visit: retrieving %s", uri)
			if c, e = v.retriever.Get(uri); e != nil {
				v.errors <- VisitErr{
					Uri:     uri,
					Message: e.Error(),
					Wrapped: e,
				}
			} else {
				v.containers <- c
			}

			<-v.semaphore
		}(uri)
	}
}
