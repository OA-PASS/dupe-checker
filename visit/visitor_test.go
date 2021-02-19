// +build integration

package visit

import (
	"dupe-checker/model"
	"dupe-checker/persistence"
	"dupe-checker/retriever"
	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"log"
	"net/http"
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
		Timeout: 120 * time.Second,
	}

	_ = &sqlite3.SQLiteDriver{}
	store, _ := persistence.NewSqlLiteStore("file:/tmp/test.db?cache=shared&mode=rw", persistence.SqliteParams{
		MaxIdleConn: 2,
		MaxOpenConn: 2,
	}, nil)

	maxSimultaneousReqs := 5

	underTest := New(retriever.New(client, "fedoraAdmin", "moo", "TestVisitor_Walk"), maxSimultaneousReqs)

	filter := func(container model.LdpContainer) bool {
		// check persistence store:
		//  if the container is processed, then don't descend.
		if state, err := store.Retrieve(container.Uri()); err == nil && state == persistence.Processed {
			return false
		}

		//  if the container is not done, then descend.
		//  if the container is not present, then descend.

		// if the container is not a pass resource, descend.
		if ok, _ := container.IsPassResource(); !ok {
			log.Printf("visit: recursing non-PASS resource %s", container.Uri())
			return true
		}

		// otherwise don't descend (i.e. it is a PASS resource, so there are no contained resources)
		return false
	}

	accept := func(container model.LdpContainer) bool {
		// check persistence store:
		//   if the container is processed, then don't accept
		if state, err := store.Retrieve(container.Uri()); err == nil && state == persistence.Processed {
			return false
		}

		if ok, passType := container.IsPassResource(); ok {
			log.Printf("visit: accepting PASS resource %s %s", container.Uri(), passType)
			return true
		}
		return false
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		underTest.Walk("http://fcrepo:8080/fcrepo/rest/policies", filter, accept)
		wg.Done()
	}()

	accepted := 0
	wg.Add(2)
	go func() {
		for container := range underTest.Containers {
			// if a container is a PASS resource, check for duplicates, store result, and mark the resource as processed.
			ok, passType := container.IsPassResource()
			assert.True(t, ok)
			assert.True(t, len(passType) > 0)
			assert.True(t, len(container.Uri()) > 0)
			log.Printf("read %s %s off channel", container.Uri(), passType)
			accepted++
			if err := store.StoreContainer(container, persistence.Processed); err != nil {
				log.Printf("%s", err.Error())
			}
		}
		wg.Done()
	}()

	go func() {
		for event := range underTest.Events {
			switch event.EventType {
			case EventDescendStartContainer:
				store.StoreUri(event.Target, persistence.Started)
			case EventDescendEndContainer:
				// when a container ends, we can check to make sure all of its children are processed and then mark
				// the container as processed.
				store.StoreUri(event.Target, persistence.Completed)
			case EventProcessedForDupes:
				store.StoreUri(event.Target, persistence.Processed)
			}
		}
		wg.Done()
	}()

	wg.Wait()
	log.Printf("Walk complete.")
	assert.Equal(t, 12, accepted)
}
