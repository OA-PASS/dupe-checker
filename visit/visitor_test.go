// +build integration

package visit

import (
	"dupe-checker/model"
	"dupe-checker/persistence"
	"dupe-checker/retriever"
	"errors"
	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"log"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

//func Test_VisitSimple(t *testing.T) {
//	client := &http.Client{
//		Timeout: 120 * time.Second,
//	}
//
//	maxSimultaneousReqs := 2
//
//	underTest := ConcurrentVisitor{
//		retriever:  retriever.New(client, "fedoraAdmin", "moo", "Test_VisitSimple"),
//		semaphore:  make(chan int, maxSimultaneousReqs),
//		uris:       make(chan string),
//		Containers: make(chan model.LdpContainer),
//		Errors:     make(chan error),
//	}
//
//	go underTest.visit()
//
//	//wg := sync.WaitGroup{}
//	//wg.Add(1)
//
//	go func() {
//		underTest.uris <- "http://fcrepo:8080/fcrepo/rest/funders"
//		underTest.uris <- "http://fcrepo:8080/fcrepo/rest/repositoryCopies"
//		underTest.uris <- "http://fcrepo:8080/fcrepo/rest/publishers"
//		close(underTest.uris)
//		//wg.Done()
//	}()
//
//	//wg.Wait()
//
//	for result := range underTest.Containers {
//		assert.NotNil(t, result)
//		assert.True(t, len(result.Contains()) > 0)
//		assert.NotNil(t, result.Uri())
//
//		ok, passResource := result.IsPassResource()
//
//		assert.False(t, ok)
//		assert.Equal(t, "", passResource)
//	}
//
//}

func TestVisitor_Walk(t *testing.T) {
	client := &http.Client{
		Timeout: 1200 * time.Second,
	}

	_ = &sqlite3.SQLiteDriver{}
	store, _ := persistence.NewSqlLiteStore("file:/tmp/test2.db?mode=rwc&cache=shared", persistence.SqliteParams{
		MaxIdleConn: 2,
		MaxOpenConn: 2,
	}, nil)
	store = persistence.NewRetrySqliteStore(store, 500*time.Millisecond, 2, 10, sqlite3.ErrBusy, sqlite3.ErrLocked, sqlite3.ErrConstraint)

	maxSimultaneousReqs := 5

	underTest := New(retriever.New(client, "fedoraAdmin", "moo", "TestVisitor_Walk"), maxSimultaneousReqs)

	filter := func(container model.LdpContainer) bool {
		// check persistence store:
		//  if the container state is Processed, then don't descend.
		//  if the container is any state other than Processed, then descend.
		if state, err := store.Retrieve(container.Uri()); err == nil && state == persistence.Processed {
			return false
		} else if err != nil && !errors.Is(err, persistence.ErrNoResults) {
			log.Printf("filter: %v", err)
		}

		// if the container is a PASS resource, don't descend (there are no contained resources)
		// TODO double check re files
		if ok, _ := container.IsPassResource(); ok {
			//log.Printf("visit: refusing to recurse PASS resource %s", container.Uri())
			return false
		}

		// if it's an acl don't descend
		if strings.HasPrefix(container.Uri(), "http://fcrepo:8080/rest/acls") || strings.Contains(container.Uri(), ".acl") {
			return false
		}

		// otherwise descend (i.e. it is not a PASS resource, and the container state is not Processed)
		return true
	}

	accept := func(container model.LdpContainer) bool {
		// check persistence store:
		//   if the container state is Processed, then don't accept
		if state, err := store.Retrieve(container.Uri()); err == nil && state == persistence.Processed {
			return false
		} else if err != nil && !errors.Is(err, persistence.ErrNoResults) {
			//log.Printf("accept: %v", err)
		}

		// if the container is a a PASS resource, accept it for processing.
		if ok, _ := container.IsPassResource(); ok {
			//log.Printf("visit: accepting PASS resource for processing %s %s", container.Uri(), passType)
			return true
		}

		// Otherwise don't accept the container for processing.
		return false
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		log.Printf("Beginning walk")
		underTest.Walk("http://fcrepo:8080/fcrepo/rest/", filter, accept)
		wg.Done()
	}()

	accepted := 0
	wg.Add(3)
	go func() {
		for container := range underTest.Containers {
			// if a container is a PASS resource, check for duplicates, store result, and mark the resource as processed.
			ok, passType := container.IsPassResource()
			assert.True(t, ok)
			assert.True(t, len(passType) > 0)
			assert.True(t, len(container.Uri()) > 0)
			//log.Printf("read %s %s off channel", container.Uri(), passType)
			accepted++
			if accepted%1000 == 0 {
				log.Printf("Processed %d resources", accepted)
			}
			if err := store.StoreContainer(container, persistence.Processed); err != nil {
				log.Printf("%s", err.Error())
			}
		}
		log.Printf("Processed %d total resources", accepted)
		wg.Done()
	}()

	go func() {
		for err := range underTest.Errors {
			// emit any errors from the Walk to the console
			log.Printf("%v", err)
		}
		wg.Done()
	}()

	go func() {
		for event := range underTest.Events {
			switch event.EventType {
			case EventDescendStartContainer:
				if err := store.StoreUri(event.Target, persistence.Started); err != nil {
					log.Printf("%v", err)
				} else {
					//log.Printf("%s", event.Message)
				}
			case EventDescendEndContainer:
				// when a container ends, we can check to make sure all of its children are processed and then mark
				// the container as processed.  Note we don't have to do this for PASS resources
				if err := store.StoreUri(event.Target, persistence.Completed); err != nil {
					log.Printf("%v", err)
				} else {
					//log.Printf("%s", event.Message)
				}

				// retrieve the full container
				// iterate its children
				// break at first child not processed
				//   technically, only URIs that are accepted should be present as children.  if for whatever reason a
				//   child resource is not accepted, it will never be processed and therefore its parent container cannot
				//   be either
				// if all children are processed, update container state to processed
			case EventProcessedContainer:
				if err := store.StoreUri(event.Target, persistence.Processed); err != nil {
					log.Printf("%v", err)
				} else {
					//log.Printf("%s", event.Message)
				}
			}
		}
		wg.Done()
	}()

	wg.Wait()
	log.Printf("Walk complete.")
	assert.Equal(t, 12, accepted)
}
