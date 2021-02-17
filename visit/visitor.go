package visit

import (
	"dupe-checker/model"
	"dupe-checker/retriever"
	"fmt"
	"log"
)

type Visitor struct {
	retriever retriever.Retriever
	semaphore chan int
	//filter func (container model.LdpContainer) bool
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

func (v Visitor) walk(uri string, errors chan error, filter func(container model.LdpContainer) bool) chan model.LdpContainer {
	containers := make(chan model.LdpContainer)

	go func() {
		for {
			v.semaphore <- 1
			go func() {
				v.visit(uri, errors, func(c model.LdpContainer) {
					<-v.semaphore

					if c.Uri() == "" {
						errors <- fmt.Errorf("visit: missing container for %s", uri)
						return
					}

					if filter == nil {
						if defaultFilter(c) {
							containers <- c
						}
					} else if filter(c) {
						containers <- c
					}

					for _, containedUri := range c.Contains() {
						v.walk(containedUri, errors, filter)
					}
				})
			}()
		}
	}()

	return containers
}

func (v Visitor) visit(uri string, errors chan error, fn func(container model.LdpContainer)) {
	var e error
	var c model.LdpContainer

	defer fn(c)

	log.Printf("visit: retrieving %s", uri)

	if c, e = v.retriever.Get(uri); e != nil {
		errors <- VisitErr{
			Uri:     uri,
			Message: e.Error(),
			Wrapped: e,
		}
	}
}
