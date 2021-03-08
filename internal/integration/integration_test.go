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

// +build integration

package integration

import (
	"bytes"
	"dupe-checker/env"
	"dupe-checker/model"
	"dupe-checker/persistence"
	"dupe-checker/query"
	"dupe-checker/retrieve"
	"dupe-checker/visit"
	"embed"
	"errors"
	"fmt"
	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
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

var (
	httpClient  http.Client
	environment = env.New()
	err         error

	//go:embed *.ttl
	ttlResources embed.FS

	//go:embed queryplan-simplejournal.json
	queryPlanSimpleJournal string

	//go:embed queryplan-orjournal.json
	queryPlanOrJournal string

	//go:embed queryplan-publication.json
	queryPlanPub string

	//go:embed queryplan-funder.json
	queryPlanFunder string

	//go:embed queryplan-grant.json
	queryPlanGrant string

	//go:embed queryplan-repocopy.json
	queryPlanRepoCopy string

	//go:embed queryplan-user.json
	queryPlanUser string

	//go:embed queryplan-submission.json
	queryPlanSubmission string

	//go:embed queryplan-publicationsandusers.json
	queryPlanPubsAndUsers string

	//go:embed queryplan-alltherest.json
	queryPlanAllTheRest string
)

func TestMain(m *testing.M) {

	if timeout, err := strconv.Atoi(environment.HttpTimeoutMs); err != nil {
		panic("Invalid integer value for HTTP_TIMEOUT_MS: " + err.Error())
	} else {
		httpClient = http.Client{
			Timeout: time.Duration(timeout) * time.Millisecond,
		}
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

	// give time for the indexer to process the newly created resources
	time.Sleep(2 * time.Second)
	// call flag.Parse() here if TestMain uses flags
	os.Exit(m.Run())
}

// Returns a MatchHandler that records it had been executed (i.e. the query processor invoked the handler), the number
// of times it was executed, and records all of the URIs found in the match and the number of times they are seen.  If
// the optional Store is provided, then the duplicates will be recorded in the persistence store.
func matchHandler(t *testing.T, executed *bool, timeInvoked *int, duplicateUris *map[string]int, store *persistence.Store) query.MatchHandler {
	return func(result interface{}) (bool, error) {
		match := result.(query.Match)
		*executed = true
		*timeInvoked++
		localDuplicatesMap := make(map[string]int)
		for _, matchingUri := range match.MatchingUris {
			if match.UriPathsEqual(matchingUri, match.PassUri) {
				continue
			}
			if _, contains := localDuplicatesMap[matchingUri]; contains {
				localDuplicatesMap[matchingUri]++
			} else {
				localDuplicatesMap[matchingUri] = 1
			}
		}

		if store != nil {
			for candidateDupe, _ := range localDuplicatesMap {
				// The resource should never be a duplicate with itself
				assert.NotEqual(t, match.StripBaseUri(match.PassUri), match.StripBaseUri(candidateDupe))
				if err := (*store).StoreDupe(match.StripBaseUri(match.PassUri), match.StripBaseUri(candidateDupe), match.PassType, match.MatchFields, match.MatchValues[candidateDupe], persistence.DupeContainerAttributes{
					SourceCreatedBy:      match.ContainerProperties.SourceCreatedBy,
					SourceCreated:        match.ContainerProperties.SourceCreated,
					SourceLastModifiedBy: match.ContainerProperties.SourceLastModifiedBy,
					SourceLastModified:   match.ContainerProperties.SourceLastModified,
				}); err != nil {
					return false, err
				}
			}
		}

		for dupeUri, localCount := range localDuplicatesMap {
			if count, exists := (*duplicateUris)[dupeUri]; exists {
				(*duplicateUris)[dupeUri] = count + localCount
			} else {
				(*duplicateUris)[dupeUri] = localCount
			}
		}

		return false, nil
	}
}

func Test_DuplicateQuerySuite(t *testing.T) {
	t.Run("Duplicates Test Suite", func(t *testing.T) {
		t.Run("duplicateFunder", findDuplicateFunder)
		t.Run("duplicateGrant", findDuplicateGrant)
		t.Run("duplicateJournalOr", findDuplicateJournal)
		t.Run("duplicateJournalSimple", findDuplicateJournalSimple)
		t.Run("duplicatePublication", findDuplicatePublication)
		t.Run("duplicateRepoCopy", findDuplicateRepoCopy)
		t.Run("duplicateSubmission", findDuplicateSubmission)
		t.Run("duplicateUser", findDuplicateUser)
	})
}

func Test_DuplicateRun(t *testing.T) {
	t.Run("Duplicate Test Run", func(t *testing.T) {
		t.Run("duplicatePubsAndUsers", findDuplicatePublicationsAndUsers)
		t.Run("duplicateAllTheRest", findDuplicateAllTheRest)
	})
}

func findDuplicatePublicationsAndUsers(t *testing.T) {
	_ = &sqlite3.SQLiteDriver{}

	store, _ := persistence.NewSqlLiteStore("file:/tmp/pubsanduserstest.db?mode=rwc&cache=shared", persistence.SqliteParams{
		MaxIdleConn: 4,
		MaxOpenConn: 4,
	}, nil)

	if err != nil {
		panic(err.Error())
	}

	plan := query.NewPlanDecoder().Decode(queryPlanPubsAndUsers)
	log.Printf("Query plan: %s", plan)

	// store candidate duplicate uris and their type in the database
	handlerExecuted := false
	potentialDuplicates := map[string]int{}
	times := 0
	matchHandler := matchHandler(t, &handlerExecuted, &times, &potentialDuplicates, &store)

	// descend into all containers that are not pass resources or acls
	filterFn := func(c model.LdpContainer) bool {
		if visit.IsAclResource(c) {
			// don't descend acl resources
			return false
		}

		if visit.IsPassResource(c) {
			// don't descend pass resources (TODO revisit this re files)
			return false
		}

		// if the container is the root container, publications container, or users container, descend
		// otherwise, don't
		switch {
		case strings.HasPrefix(c.Uri(), fmt.Sprintf("%s/publications", environment.FcrepoBaseUri)):
			fallthrough
		case strings.HasPrefix(c.Uri(), fmt.Sprintf("%s/users", environment.FcrepoBaseUri)):
			fallthrough
		case c.Uri() == fmt.Sprintf("%s/", environment.FcrepoBaseUri):
			fallthrough
		case c.Uri() == fmt.Sprintf("%s", environment.FcrepoBaseUri):
			return true
		default:
			return false
		}
	}

	// accept all pass resources for processing
	acceptFn := func(c model.LdpContainer) bool {
		// accept PASS resources for processing (i.e. accepted resources will be sent to the visitor.Containers channel)
		return visit.IsPassResource(c)
	}

	// process accepted containers (i.e. PASS resources) by querying for their duplicates
	containerHandler := func(c model.LdpContainer) {
		var queryPlan query.Plan

		// Select the query plan based on the type of resource
		if isPass, passType := c.IsPassResource(); isPass {
			switch passType {
			case model.PassTypeUser:
				queryPlan = plan[model.PassTypeUser]
			case model.PassTypePublication:
				queryPlan = plan[model.PassTypePublication]
			default:
				panic("Unsupported type " + passType)
			}
		} else {
			panic(fmt.Sprintf("Container not a PASS resource %v", c))
		}

		// Execute the query and hand the result off to the match handler
		// The match handler is responsible for processing the result (i.e. determining if there are duplicates and
		// persisting them in the database).
		if _, err := queryPlan.Execute(c, matchHandler); err != nil {
			// allow for errors where keys cannot be extracted, this is to be expected with our tests
			if !errors.Is(err, query.ErrMissingRequiredKey) {
				log.Printf("Error performing query: %s", err.Error())
			}
		}
	}

	retriever := retrieve.New(&httpClient, environment.FcrepoUser, environment.FcrepoPassword, "findDuplicatePublicationsAndUsers")
	maxReq, err := strconv.Atoi(environment.FcrepoMaxConcurrentRequests)
	assert.Nil(t, err)

	controller := visit.NewController(retriever, maxReq)
	controller.ErrorHandler(visit.LogErrorHandler)
	controller.EventHandler(visit.NoopEventHandler)
	controller.ContainerHandler(containerHandler)
	controller.Begin(environment.FcrepoBaseUri, acceptFn, filterFn)
}

func findDuplicateAllTheRest(t *testing.T) {
	_ = &sqlite3.SQLiteDriver{}

	store, _ := persistence.NewSqlLiteStore("file:/tmp/pubsanduserstest.db?mode=rwc&cache=shared", persistence.SqliteParams{
		MaxIdleConn: 4,
		MaxOpenConn: 4,
	}, nil)

	if err != nil {
		panic(err.Error())
	}

	plan := query.NewPlanDecoder().Decode(queryPlanAllTheRest)
	log.Printf("Query plan: %s", plan)

	// store candidate duplicate uris and their type in the database
	handlerExecuted := false
	potentialDuplicates := map[string]int{}
	times := 0
	matchHandler := matchHandler(t, &handlerExecuted, &times, &potentialDuplicates, &store)

	// descend into all containers that are not pass resources or acls
	filterFn := func(c model.LdpContainer) bool {
		if visit.IsAclResource(c) {
			// don't descend acl resources
			return false
		}

		if visit.IsPassResource(c) {
			// don't descend pass resources (TODO revisit this re files)
			return false
		}

		// if the container is the root container, funders, grants, repositoryCopies, submissions, or journals
		// container, descend.
		// otherwise, don't
		switch {
		case strings.HasPrefix(c.Uri(), fmt.Sprintf("%s/funders", environment.FcrepoBaseUri)):
			fallthrough
		case strings.HasPrefix(c.Uri(), fmt.Sprintf("%s/grants", environment.FcrepoBaseUri)):
			fallthrough
		case strings.HasPrefix(c.Uri(), fmt.Sprintf("%s/repositoryCopies", environment.FcrepoBaseUri)):
			fallthrough
		case strings.HasPrefix(c.Uri(), fmt.Sprintf("%s/submissions", environment.FcrepoBaseUri)):
			fallthrough
		case strings.HasPrefix(c.Uri(), fmt.Sprintf("%s/journals", environment.FcrepoBaseUri)):
			fallthrough
		case c.Uri() == fmt.Sprintf("%s/", environment.FcrepoBaseUri):
			fallthrough
		case c.Uri() == fmt.Sprintf("%s", environment.FcrepoBaseUri):
			return true
		default:
			return false
		}
	}

	// accept all pass resources for processing
	acceptFn := func(c model.LdpContainer) bool {
		// accept PASS resources for processing (i.e. accepted resources will be sent to the visitor.Containers channel)
		return visit.IsPassResource(c)
	}

	// process accepted containers (i.e. PASS resources) by querying for their duplicates
	containerHandler := func(c model.LdpContainer) {
		var queryPlan query.Plan

		// Select the query plan based on the type of resource
		if isPass, passType := c.IsPassResource(); isPass {
			switch passType {
			case model.PassTypeFunder:
				fallthrough
			case model.PassTypeGrant:
				fallthrough
			case model.PassTypeJournal:
				fallthrough
			case model.PassTypeRepositoryCopy:
				fallthrough
			case model.PassTypeSubmission:
				queryPlan = plan[passType]
			default:
				panic("Unsupported type " + passType)
			}
		} else {
			panic(fmt.Sprintf("Container not a PASS resource %v", c))
		}

		// Execute the query and hand the result off to the match handler
		// The match handler is responsible for processing the result (i.e. determining if there are duplicates and
		// persisting them in the database).
		if _, err := queryPlan.Execute(c, matchHandler); err != nil {
			// allow for errors where keys cannot be extracted, this is to be expected with our tests
			if !errors.Is(err, query.ErrMissingRequiredKey) {
				log.Printf("Error performing query: %s", err.Error())
			}
		}
	}

	retriever := retrieve.New(&httpClient, environment.FcrepoUser, environment.FcrepoPassword, "findDuplicateAllTheRest")
	maxReq, err := strconv.Atoi(environment.FcrepoMaxConcurrentRequests)
	assert.Nil(t, err)

	controller := visit.NewController(retriever, maxReq)
	controller.ErrorHandler(visit.LogErrorHandler)
	controller.EventHandler(visit.NoopEventHandler)
	controller.ContainerHandler(containerHandler)
	controller.Begin(environment.FcrepoBaseUri, acceptFn, filterFn)
}

func findDuplicateSubmission(t *testing.T) {
	t.Parallel()
	queryPlan := query.NewPlanDecoder().Decode(queryPlanAllTheRest)[model.PassTypeSubmission]
	log.Printf("Query plan: %s", queryPlan)
	handlerExecuted := false
	potentialDuplicates := map[string]int{}
	times := 0

	matchHandler := matchHandler(t, &handlerExecuted, &times, &potentialDuplicates, nil)

	executeQueryPlan(t, queryPlan, fmt.Sprintf("%s/%s", environment.FcrepoBaseUri, "submissions"), "http://oapass.org/ns/pass#Submission", matchHandler, nil)
	assert.True(t, handlerExecuted) // that we executed the handler - and its assertions therein - supplied to the queryPlan at least once
	assert.Equal(t, 4, times)
	assert.Equal(t, 3, len(potentialDuplicates)) // for the two duplicate User resources
}

func findDuplicateUser(t *testing.T) {
	t.Parallel()

	//_ = &sqlite3.SQLiteDriver{}
	//
	//store, _ := persistence.NewSqlLiteStore("file:/tmp/pubsanduserstest.db?mode=rwc&cache=shared", persistence.SqliteParams{
	//	MaxIdleConn: 4,
	//	MaxOpenConn: 4,
	//}, nil)

	queryPlan := query.NewPlanDecoder().Decode(queryPlanPubsAndUsers)[model.PassTypeUser]
	log.Printf("Query plan: %s", queryPlan)
	handlerExecuted := false
	potentialDuplicates := map[string]int{}
	times := 0

	matchHandler := matchHandler(t, &handlerExecuted, &times, &potentialDuplicates, nil)

	executeQueryPlan(t, queryPlan, fmt.Sprintf("%s/%s", environment.FcrepoBaseUri, "users"), "http://oapass.org/ns/pass#User", matchHandler, nil)
	assert.True(t, handlerExecuted) // that we executed the handler - and its assertions therein - supplied to the queryPlan at least once
	assert.Equal(t, 2, times)
	assert.Equal(t, 2, len(potentialDuplicates)) // for the two duplicate User resources
}

func findDuplicateRepoCopy(t *testing.T) {
	t.Parallel()
	queryPlan := query.NewPlanDecoder().Decode(queryPlanAllTheRest)[model.PassTypeRepositoryCopy]
	log.Printf("Query plan: %s", queryPlan)
	handlerExecuted := false
	potentialDuplicates := map[string]int{}
	times := 0

	matchHandler := matchHandler(t, &handlerExecuted, &times, &potentialDuplicates, nil)

	executeQueryPlan(t, queryPlan, fmt.Sprintf("%s/%s", environment.FcrepoBaseUri, "repositoryCopies"), "http://oapass.org/ns/pass#RepositoryCopy", matchHandler, nil)
	assert.True(t, handlerExecuted)              // that we executed the handler - and its assertions therein - supplied to the queryPlan at least once
	assert.Equal(t, 4, times)                    // one query for each resource plus an additional query for the resource with both the url and the publication
	assert.Equal(t, 3, len(potentialDuplicates)) // for the three duplicate RepoCopy resources
}

func findDuplicateGrant(t *testing.T) {
	t.Parallel()
	queryPlan := query.NewPlanDecoder().Decode(queryPlanAllTheRest)[model.PassTypeGrant]
	log.Printf("Query plan: %s", queryPlan)
	handlerExecuted := false
	potentialDuplicates := map[string]int{}
	times := 0

	matchHandler := matchHandler(t, &handlerExecuted, &times, &potentialDuplicates, nil)

	executeQueryPlan(t, queryPlan, fmt.Sprintf("%s/%s", environment.FcrepoBaseUri, "grants"), "http://oapass.org/ns/pass#Grant", matchHandler, nil)
	assert.True(t, handlerExecuted) // that we executed the handler - and its assertions therein - supplied to the queryPlan at least once
	assert.Equal(t, 2, times)
	assert.Equal(t, 2, len(potentialDuplicates)) // for the two duplicate Funder resources
}

func findDuplicateFunder(t *testing.T) {
	t.Parallel()
	queryPlan := query.NewPlanDecoder().Decode(queryPlanAllTheRest)[model.PassTypeFunder]
	log.Printf("Query plan: %s", queryPlan)
	handlerExecuted := false
	potentialDuplicates := map[string]int{}
	times := 0

	matchHandler := matchHandler(t, &handlerExecuted, &times, &potentialDuplicates, nil)

	executeQueryPlan(t, queryPlan, fmt.Sprintf("%s/%s", environment.FcrepoBaseUri, "funders"), "http://oapass.org/ns/pass#Funder", matchHandler, nil)
	assert.True(t, handlerExecuted) // that we executed the handler - and its assertions therein - supplied to the queryPlan at least once
	assert.Equal(t, 2, times)
	assert.Equal(t, 2, len(potentialDuplicates)) // for the two duplicate Funder resources
}

func findDuplicatePublication(t *testing.T) {
	t.Parallel()
	queryPlan := query.NewPlanDecoder().Decode(queryPlanPubsAndUsers)[model.PassTypePublication]
	log.Printf("Query plan: %s", queryPlan)
	handlerExecuted := false
	potentialDuplicates := map[string]int{}
	times := 0

	matchHandler := matchHandler(t, &handlerExecuted, &times, &potentialDuplicates, nil)

	executeQueryPlan(t, queryPlan, fmt.Sprintf("%s/%s", environment.FcrepoBaseUri, "publications"), "http://oapass.org/ns/pass#Publication", matchHandler, nil)
	assert.True(t, handlerExecuted)              // that we executed the handler - and its assertions therein - supplied to the queryPlan at least once
	assert.Equal(t, 6, times)                    // 1 query each for four resources, plus a two additional queries for the resource with all three properties
	assert.Equal(t, 4, len(potentialDuplicates)) // for the four duplicate Publication resources
}

func findDuplicateJournalSimple(t *testing.T) {
	t.Parallel()
	journalQueryPlan := query.NewPlanDecoder().Decode(queryPlanSimpleJournal)[model.PassTypeJournal]
	handlerExecuted := false
	potentialDuplicates := map[string]int{}
	times := 0

	matchHandler := matchHandler(t, &handlerExecuted, &times, &potentialDuplicates, nil)

	executeQueryPlan(t, journalQueryPlan, fmt.Sprintf("%s/%s", environment.FcrepoBaseUri, "journals"), "http://oapass.org/ns/pass#Journal", matchHandler, nil)
	assert.True(t, handlerExecuted) // that we executed the handler - and its assertions therein - supplied to the journalQueryPlan at least once
	assert.Equal(t, 2, times)       // for the two Journal resources that contain the 'nlmta' key (the third Journal resource does not)
	assert.Equal(t, 2, len(potentialDuplicates))
}

func findDuplicateJournal(t *testing.T) {
	t.Parallel()
	journalQueryPlan := query.NewPlanDecoder().Decode(queryPlanAllTheRest)[model.PassTypeJournal]
	handlerExecuted := false
	times := 0
	potentialDuplicates := map[string]int{}

	// matchHandler is executed once for *each* query
	//  - we always expect at least one result, because the query that looks for duplicates will find at least the
	//    original resource
	//  - any matches beyond that are considered potential duplicates
	matchHandler := matchHandler(t, &handlerExecuted, &times, &potentialDuplicates, nil)

	executeQueryPlan(t, journalQueryPlan, fmt.Sprintf("%s/%s", environment.FcrepoBaseUri, "journals"), "http://oapass.org/ns/pass#Journal", matchHandler, nil)
	assert.True(t, handlerExecuted)              // that we executed the handler - and its assertions therein - supplied to the journalQueryPlan at least once
	assert.Equal(t, 3, len(potentialDuplicates)) // we expect three potential duplicates; the journal with the single ISSN won't be found because we exact match on ISSNs, this is a TODO/FIXME
	assert.Equal(t, 5, times)                    // the match handler executed once for each query that was performed.
}

func executeQueryPlan(t *testing.T, queryPlan query.Plan, startUri string, passType string, matchHandler func(result interface{}) (bool, error), containerHandler func(model.LdpContainer)) {
	retriever := retrieve.New(&httpClient, environment.FcrepoUser, environment.FcrepoPassword, "test_findDuplicateJournal")
	maxReq, err := strconv.Atoi(environment.FcrepoMaxConcurrentRequests)
	assert.Nil(t, err)

	controller := visit.NewController(retriever, maxReq)
	controller.ErrorHandler(visit.NoopErrorHandler)
	controller.EventHandler(visit.NoopEventHandler)
	if containerHandler == nil {
		controller.ContainerHandler(defaultContainerHandler(t, queryPlan, passType, matchHandler))
	} else {
		controller.ContainerHandler(containerHandler)
	}
	controller.Begin(startUri, visit.AcceptAllFilter, visit.AcceptAllFilter)
}

var defaultContainerHandler = func(t *testing.T, queryPlan query.Plan, passType string, matchHandler func(result interface{}) (bool, error)) func(c model.LdpContainer) {
	return func(c model.LdpContainer) {
		log.Printf(">> Container: %s (%s)", c.Uri(), c.PassType())
		if isPass, candidate := c.IsPassResource(); isPass && candidate == passType {
			// note that if the container URI has been flagged as a duplicate in a previous invocation, then this
			// invocation is redundant
			if _, err := queryPlan.Execute(c, matchHandler); err != nil {
				// allow for errors where keys cannot be extracted, this is to be expected with our tests
				if !errors.Is(err, query.ErrMissingRequiredKey) {
					assert.Failf(t, "Error performing query: %s", err.Error())
				}
			}
		}
	}
}
