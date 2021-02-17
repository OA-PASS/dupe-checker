package visit

import (
	"dupe-checker/model"
	"dupe-checker/retriever"
	"log"
	"net/http"
	"testing"
	"time"
)

func Test_VisitSimple(t *testing.T) {
	errors := make(chan error)
	client := &http.Client{
		Timeout: 120 * time.Second,
	}

	maxSimultaneousReqs := 5


	underTest := Visitor{
		retriever: retriever.New(client, "fedoraAdmin", "moo", "Test_VisitSimple"),
		semaphore: make(chan int, maxSimultaneousReqs),
	}

	passResources := underTest.walk("http://fcrepo:8080/fcrepo/rest/submissions", errors, func(container model.LdpContainer) bool {
		if ok, passType := container.IsPassResource(); ok {
			log.Printf("filter found PASS resource %s %s", container.Uri(), passType)
			return true
		}
		return false
	})

	for passResource := range passResources {
		log.Printf("read pass resource %s off channel", passResource.Uri())
	}

}
