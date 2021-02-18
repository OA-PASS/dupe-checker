package visit

import (
	"dupe-checker/model"
	"dupe-checker/retriever"
	"fmt"
	"log"
	"sync"
)

type visitor struct {
	retriever  retriever.Retriever
	semaphore  chan int
	Containers chan model.LdpContainer
	Errors     chan error
}

type VisitErr struct {
	Uri     string
	Message string
	Wrapped error
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

func (ve VisitErr) Error() string {
	return fmt.Sprintf("visit: error visiting uri %s, %s", ve.Uri, ve.Message)
}

func (ve VisitErr) Unwrap() error {
	return ve.Wrapped
}

func New(retriever retriever.Retriever, maxConcurrent int) visitor {
	return visitor{
		retriever:  retriever,
		semaphore:  make(chan int, maxConcurrent),
		Containers: make(chan model.LdpContainer),
		Errors:     make(chan error),
	}
}

func (v visitor) Walk(uri string, filter, accept func(container model.LdpContainer) bool) {
	var c model.LdpContainer
	var e error

	if c, e = v.retriever.Get(uri); e != nil {
		log.Fatalf("visit: error retrieving %s: %s", uri, e.Error())
		return
	}

	if c.Uri() == "" {
		log.Fatalf("visit: missing container for %s", uri)
		return
	}

	if filter == nil {
		filter = defaultFilter
	}

	if accept == nil {
		accept = defaultAccept
	}

	v.walkInternal(c, filter, accept)

	close(v.Containers)
	close(v.Errors)
}

func (v visitor) walkInternal(c model.LdpContainer, filter, accept func(container model.LdpContainer) bool) {
	var e error
	wg := sync.WaitGroup{}
	wg.Add(len(c.Contains()))
	for _, uri := range c.Contains() {
		v.semaphore <- 1
		go func(uri string) {
			log.Printf("visit: retrieving %s", uri)
			if c, e = v.retriever.Get(uri); e != nil {
				<-v.semaphore
				v.Errors <- fmt.Errorf("%v", VisitErr{
					Uri:     uri,
					Message: e.Error(),
					Wrapped: e,
				})
			} else {
				<-v.semaphore
				if accept(c) {
					v.Containers <- c
				}
				if filter(c) {
					v.walkInternal(c, filter, accept)
				}
			}
			wg.Done()
		}(uri)
	}
	wg.Wait()
}
