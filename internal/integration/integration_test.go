// +build integration

package integration

import (
	"bytes"
	"dupe-checker/env"
	"dupe-checker/model"
	"dupe-checker/query"
	"dupe-checker/retrieve"
	"dupe-checker/visit"
	"embed"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

var serviceDeps = map[string]bool{
	"activemq:8161":      false,
	"activemq:61613":     false,
	"activemq:61616":     false,
	"fcrepo:8080":        false,
	"elasticsearch:9200": false,
}

var httpClient http.Client

var environment = env.New()

var err error

//go:embed *.ttl
var ttlResources embed.FS

//go:embed queryplan-simplejournal.json
var queryPlanSimpleJournal string

//go:embed queryplan-orjournal.json
var queryPlanOrJournal string

func TestMain(m *testing.M) {

	httpClient = http.Client{
		Timeout: 10 * time.Second,
	}

	// Fedora, ElasticSearch, ActiveMQ and the Indexer all need to be up.

	// Verify tcp connectivity to dependencies

	skipDeps, _ := strconv.ParseBool(environment.ItSkipServiceDepCheck)
	if !skipDeps {
		wg := sync.WaitGroup{}
		wg.Add(len(serviceDeps))
		mu := sync.Mutex{}

		for hostAndPort := range serviceDeps {
			go func(hostAndPort string) {
				timeout := 5 * time.Second
				start := time.Now()

				for !timedout(start, timeout) {
					fmt.Printf("Dialing %v\n", hostAndPort)
					if c, err := net.Dial("tcp", hostAndPort); err == nil {
						_ = c.Close()
						mu.Lock()
						serviceDeps[hostAndPort] = true
						mu.Unlock()
						fmt.Printf("Successfully connected to %v\n", hostAndPort)
						wg.Done()
						break
					} else {
						time.Sleep(500 * time.Millisecond)
						if timedout(start, timeout) {
							wg.Done()
							break
						}
					}
				}
			}(hostAndPort)
		}

		wg.Wait()

		for k, v := range serviceDeps {
			if !v {
				fmt.Printf("failed to connect to %v", k)
				os.Exit(-1)
			}
		}
	}

	// Create parent containers, http://fcrepo:8080/fcrepo/rest/journals, http://fcrepo:8080/fcrepo/rest/users, etc.
	// Populate them with test resources.
	// If Fedora already has a container, skip the initialization of resources for that container.
	for _, containerName := range []string{"journals", "users", "publications", "grants", "funders", "repositoryCopies", "submissions"} {
		url := fmt.Sprintf("%s/%s", environment.FcrepoBaseUri, containerName)
		req, _ := http.NewRequest("HEAD", url, nil)
		if err := perform(req, 200); err == nil {
			log.Printf("Container %s already exists, skipping initialization", url)
			continue
		}
		req, _ = http.NewRequest("PUT", url, nil)
		req.Header.Add("Content-Type", "application/n-triples")
		if err := perform(req, 201); err != nil {
			log.Fatalf("Error creating container %s: %s", url, err.Error())
		}

		// Create resources in Fedora for the given container, at least two duplicates of each resource:
		//  Journal
		//  Publication
		//  User
		//  Grant
		//  Publisher
		//  Funder
		//  RepositoryCopy
		//  User
		//  Submission
		//
		// Some resources have multiple criteria; e.g a duplicate Publication can be found by (DOI or PMID) or Title.  A
		// duplicate Journal can be found by NLMTA or (Journal Name and ISSN).  Ideally there would be duplicates that
		// satisfy each criteria (e.g. a duplicate Journal with the same NLMTA, and a duplicate Journal with the name
		// and ISSN.

		for _, testResource := range testResources(fmt.Sprintf("pass-%s*.ttl", containerName), ttlResources) {
			resource, _ := ttlResources.ReadFile(testResource.Name())
			url := fmt.Sprintf("%s/%s", environment.FcrepoBaseUri, containerName)
			req, _ := http.NewRequest("POST", url, bytes.NewReader(resource))
			req.Header.Add("Content-Type", "text/turtle")
			if err := perform(req, 201); err != nil {
				log.Fatalf("Error creating repository resource under %s from %s: %s", url,
					testResource.Name(), err.Error())
			} else {
				log.Printf("Created test resource in repository under %s from %s", url, testResource.Name())
			}
		}
	}

	// call flag.Parse() here if TestMain uses flags
	os.Exit(m.Run())
}

func Test_FindDuplicateJournalSimple(t *testing.T) {
	journalQueryPlan := query.NewPlanDecoder().Decode(queryPlanSimpleJournal)["http://oapass.org/ns/pass#Journal"]
	handlerExecuted := false
	potentialDuplicates := map[string]int{}
	times := 0

	matchHandler := func(result interface{}) (bool, error) {
		match := result.(query.Match)
		handlerExecuted = true
		times++
		for _, matchingUri := range match.MatchingUris {
			if matchingUri == match.PassUri {
				continue
			}
			if _, contains := potentialDuplicates[matchingUri]; contains {
				potentialDuplicates[matchingUri]++
			} else {
				potentialDuplicates[matchingUri] = 1
			}
		}
		return true, nil // we return true here because in an 'or' scenario - which we aren't in for this test
		// - we could short-circuit the plan, because we found two hits for the container (i.e., there's a
		// duplicate)
	}
	executeJournalQueryPlan(t, journalQueryPlan, matchHandler)
	assert.True(t, handlerExecuted) // that we executed the handler - and its assertions therein - supplied to the journalQueryPlan at least once
	assert.Equal(t, 2, times)       // for the two Journal resources that contain the 'nlmta' key (the third Journal resource does not)
	assert.Equal(t, 2, len(potentialDuplicates))
}

func Test_FindDuplicateJournalOr(t *testing.T) {
	journalQueryPlan := query.NewPlanDecoder().Decode(queryPlanOrJournal)["http://oapass.org/ns/pass#Journal"]
	handlerExecuted := false
	times := 0
	potentialDuplicates := map[string]int{}

	// matchHandler is executed once for *each* query
	//  - we always expect at least one result, because the query that looks for duplicates will find at least the
	//    original resource
	//  - any matches beyond that are considered potential duplicates
	matchHandler := func(result interface{}) (bool, error) {
		match := result.(query.Match)
		handlerExecuted = true
		times++
		for _, matchingUri := range match.MatchingUris {
			if matchingUri == match.PassUri {
				continue
			}
			if _, contains := potentialDuplicates[matchingUri]; contains {
				potentialDuplicates[matchingUri]++
			} else {
				potentialDuplicates[matchingUri] = 1
			}
		}
		return false, nil // we return true here because in an 'or' scenario - which we aren't in for this test
		// - we could short-circuit the plan, because we found three hits for the container (i.e., there are two
		// duplicates)
	}
	executeJournalQueryPlan(t, journalQueryPlan, matchHandler)
	assert.True(t, handlerExecuted)              // that we executed the handler - and its assertions therein - supplied to the journalQueryPlan at least once
	assert.Equal(t, 3, len(potentialDuplicates)) // we expect three potential duplicates
	assert.Equal(t, 3, times)                    // the match handler executed once for each journal TODO verify this is correct behavior when we are returning false from the handler.
}

func executeJournalQueryPlan(t *testing.T, journalQueryPlan query.Plan, matchHandler func(result interface{}) (bool, error)) {
	retriever := retrieve.New(&httpClient, environment.FcrepoUser, environment.FcrepoPassword, "Test_FindDuplicateJournal")
	maxReq, err := strconv.Atoi(environment.FcrepoMaxConcurrentRequests)
	assert.Nil(t, err)

	visitor := visit.New(retriever, maxReq)
	controller := visitController{}
	controller.errorHandler(func(e error) {
		log.Printf("Error: %s", e.Error())
	})
	controller.eventHandler(func(e visit.Event) {
		log.Printf("Event: %v", e)
	})
	controller.containerHandler(func(c model.LdpContainer) {
		log.Printf("Container: %s (%s)", c.Uri(), c.PassType())
		if isPass, passType := c.IsPassResource(); isPass && passType == "http://oapass.org/ns/pass#Journal" {
			// note that if the container URI has been flagged as a duplicate in a previous invocation, then this
			// invocation is redundant
			if _, err := journalQueryPlan.Execute(c, matchHandler); err != nil {
				// allow for errors where keys cannot be extracted, this is to be expected with our tests
				if !errors.Is(err, query.ErrMissingRequiredKey) {
					assert.Failf(t, "Error performing query: %s", err.Error())
				}
			}
		}
	})

	controller.begin(visitor, fmt.Sprintf("%s/%s", environment.FcrepoBaseUri, "journals"), visit.AcceptAllFilter, visit.AcceptAllFilter)
}

type visitController struct {
	wg              sync.WaitGroup
	errorReader     func(e error)
	eventReader     func(e visit.Event)
	containerReader func(c model.LdpContainer)
}

func (cr *visitController) errorHandler(handler func(error)) {
	if cr.errorReader != nil {
		panic("illegal state: existing error handler")
	}
	cr.wg.Add(1)
	cr.errorReader = handler
}

func (cr *visitController) eventHandler(handler func(visit.Event)) {
	if cr.eventReader != nil {
		panic("illegal state: existing event handler")
	}
	cr.wg.Add(1)
	cr.eventReader = handler
}

func (cr *visitController) containerHandler(handler func(model.LdpContainer)) {
	if cr.containerReader != nil {
		panic("illegal state: existing container handler")
	}
	cr.wg.Add(1)
	cr.containerReader = handler
}

func (cr *visitController) begin(visitor visit.ConcurrentVisitor, startingUri string, acceptFn func(container model.LdpContainer) bool, filterFn func(container model.LdpContainer) bool) {
	if cr.errorReader != nil {
		go func() {
			for err := range visitor.Errors {
				cr.errorReader(err)
			}
			cr.wg.Done()
		}()
	}

	if cr.eventReader != nil {
		go func() {
			for event := range visitor.Events {
				cr.eventReader(event)
			}

			cr.wg.Done()
		}()
	}

	if cr.containerReader != nil {
		go func() {
			for container := range visitor.Containers {
				cr.containerReader(container)
			}

			cr.wg.Done()
		}()
	}

	cr.wg.Add(1)
	go func() {
		visitor.Walk(startingUri, filterFn, acceptFn)
		cr.wg.Done()
	}()

	cr.wg.Wait()
}
