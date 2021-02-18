package visit

import (
	"dupe-checker/retriever"
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
//	underTest := visitor{
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

	maxSimultaneousReqs := 5

	underTest := New(retriever.New(client, "fedoraAdmin", "moo", "TestVisitor_Walk"), maxSimultaneousReqs)

	//filter := func(container model.LdpContainer) bool {
	//	if ok, _ := container.IsPassResource(); !ok {
	//		log.Printf("visit: recursing non-PASS resource %s", container.Uri())
	//		return true
	//	}
	//	return false
	//}
	//
	//accept := func(container model.LdpContainer) bool {
	//	if ok, passType := container.IsPassResource(); ok {
	//		log.Printf("visit: accepting PASS resource %s %s", container.Uri(), passType)
	//		return true
	//	}
	//	return false
	//}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		underTest.Walk("http://fcrepo:8080/fcrepo/rest/policies", nil, nil)
		wg.Done()
	}()

	accepted := 0
	wg.Add(1)
	go func() {
		for container := range underTest.Containers {
			ok, passType := container.IsPassResource()
			assert.True(t, ok)
			assert.True(t, len(passType) > 0)
			assert.True(t, len(container.Uri()) > 0)
			log.Printf("read %s %s off channel", container.Uri(), passType)
			accepted++
		}
		wg.Done()
	}()

	wg.Wait()
	log.Printf("Walk complete.")
	assert.Equal(t, 12, accepted)
}
