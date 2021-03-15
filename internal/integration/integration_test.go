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
	"bufio"
	"bytes"
	"dupe-checker/env"
	"dupe-checker/model"
	"dupe-checker/persistence"
	"dupe-checker/query"
	"dupe-checker/retrieve"
	"dupe-checker/visit"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/knakk/rdf"
	"github.com/logrusorgru/aurora/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

var (
	// the http client shared by the ITs
	httpClient http.Client
	// the persistence store shared by the ITs
	sharedStore *persistence.Store
	// the environment shared by the ITs
	environment = env.New()
	err         error
	// A map of resources that are in the repository prior to the execution of tests.  This map may be populated from
	// existing resources, or resources that were created as part of the initialization in TestMain.  If a container
	// existed prior to the initialization in TestMain, the passType.preExists flag will be set to true.
	resources = containerMap(make(map[passType][]string))

	//go:embed *.ttl
	ttlResources embed.FS

	//go:embed queryplan-simplejournal.json
	queryPlanSimpleJournal string

	//go:embed queryplan-publicationsandusers.json
	queryPlanPubsAndUsers string

	//go:embed queryplan-alltherest.json
	queryPlanAllTheRest string
)

type duplicateTestResult struct {
	dupes         map[string]int
	executed      bool
	expectedTimes int
	times         int
}

// Encapsulates static properties regarding a PASS type
type passType struct {
	// The name of the container that carries the type, e.g. "submissions", "publications", etc.  Meant to be used
	// as-is, in combination with env.FcrepoBaseUri to form URIs.
	containerName string
	// The name of the type, e.g. "Submission", "Publication", etc.  Meant to be used as-is in combination with
	// model.PassResourceUriPrefix to form URIs, especially the object of the rdf:type predicate.
	typeName string
	// The full rdf:type URI of the PASS type, e.g. "http://oapass.org/ns/pass#Submission" or
	// "http://oapass.org/ns/pass#Publication"
	typeUri string
	// Whether the container exists in the Fedora repository upon starting integration tests.  If a container already
	// exists in the repository, it may be preserved (i.e. not deleted) when the test concludes.
	preExists bool
}

// Manages resource URIs keyed by type
type containerMap map[passType][]string

type containerList []passType

// Returns a slice of uris associated with the supplied name (e.g. "Submission", "Publication", etc) or type (e.g.
// "http://oapass.org/ns/pass#Submission" or "http://oapass.org/ns/pass#Publication")
func (cm containerMap) get(nameOrType string) []string {
	for passType := range cm {
		if passType.containerName == nameOrType ||
			passType.typeName == nameOrType {
			return cm[passType]
		}
	}
	return []string{}
}

func (cl containerList) get(nameOrType string) *passType {
	for _, passType := range cl {
		if passType.containerName == nameOrType ||
			passType.typeName == nameOrType {
			return &passType
		}
	}
	return nil
}

// Enumerates the PASS types used by the integration tests and sets their initial values.
var typeContainers = containerList([]passType{
	{"submissions", "Submission", model.PassTypeSubmission, false},
	{"publications", "Publication", model.PassTypePublication, false},
	{"users", "User", model.PassTypeUser, false},
	{"repositoryCopies", "RepositoryCopy", model.PassTypeRepositoryCopy, false},
	{"journals", "Journal", model.PassTypeJournal, false},
	{"grants", "Grant", model.PassTypeGrant, false},
	{"funders", "Funder", model.PassTypeFunder, false},
})

// Initializes shared resources like the HTTP client and persistence Store used by the ITs, and manages the creation and
// cleanup of test resources.
//
// If a container exists upon startup, then the contents of the container are preserved on shutdown.  Containers (and
// their contents) are otherwise removed unless the environment variable `IT_PRESERVE_STATE` is 'true'.
//
// If `IT_PRESERVE_STATE` is 'true', a new Sqlite database will be initialized and used for the ITs.  Otherwise an in-
// memory implementation is used.
func TestMain(m *testing.M) {

	if timeout, err := strconv.Atoi(environment.HttpTimeoutMs); err != nil {
		panic("Invalid integer value for HTTP_TIMEOUT_MS: " + err.Error())
	} else {
		httpClient = http.Client{
			Timeout: time.Duration(timeout) * time.Millisecond,
		}
	}

	serviceDeps := map[string]bool{
		"activemq:8161":      false,
		"activemq:61613":     false,
		"activemq:61616":     false,
		"fcrepo:8080":        false,
		"elasticsearch:9200": false,
	}
	checkDependentServices(&serviceDeps)

	if isPreserveState() {
		log.Printf(aurora.Sprintf("%s %s %s env var %s is %s: state in the Fedora repository is carried over "+
			"between test suites, and may introduce false negatives if running more than one test suite at a time.",
			aurora.BgRed("***"), aurora.Red("WARNING"), aurora.BgRed("***"),
			aurora.BrightGreen(env.IT_PRESERVE_STATE), aurora.Green(environment.ItPreserveState)))
	}

	// call flag.Parse() here if TestMain uses flags
	os.Exit(m.Run())
}

func Test_DuplicateQuerySuite(t *testing.T) {
	initializeContainersAndResources()
	createPersistenceStore()

	t.Run("Duplicates Test Suite", func(t *testing.T) {
		t.Run("duplicateFunder", findDuplicateFunder)
		t.Run("duplicateGrant", findDuplicateGrant)
		t.Run("duplicateJournalOr", findDuplicateJournal)
		// earlier each test func was using its own Store.  now the store is shared by all these funcs, so in order for
		// them not to see results they shouldn't there can only be one test func per type.  so we comment out the
		// second Journal related func for now.
		//t.Run("duplicateJournalSimple", findDuplicateJournalSimple)
		t.Run("duplicatePublication", findDuplicatePublication)
		t.Run("duplicateRepoCopy", findDuplicateRepoCopy)
		t.Run("duplicateSubmission", findDuplicateSubmission)
		t.Run("duplicateUser", findDuplicateUser)
	})

	cleanup()
}

func Test_DuplicateRun(t *testing.T) {
	initializeContainersAndResources()
	createPersistenceStore()

	t.Run("Duplicate Test Run", func(t *testing.T) {
		t.Run("duplicatePubsAndUsers", findDuplicatePublicationsAndUsers)
		t.Run("duplicateAllTheRest", findDuplicateAllTheRest)
	})

	cleanup()
}

func Test_QueryExpansion(t *testing.T) {
	initializeContainersAndResources()
	createPersistenceStore()

	var (
		err error
		req *http.Request
		res *http.Response
		buf bytes.Buffer

		s         []rdf.Triple
		sUri      string
		sprime    []rdf.Triple
		sprimeUri string
		psub1     string
		psub2     string
	)

	// loading the static test resources is already completed in TestMain, but we need to craft additional duplicates to
	// test query expansion.

	// In order to stage the repository for this test, there needs to be at least one pair of Submissions that are
	// identical, except that one points to one Publication and the other Submission points to a *duplicate* of the
	// Publication.  So:
	//   1a. Find the static Submission (call it 's', already in the repo, populated by TestMain) that has a single
	//       submitter and single publication (call it 'psub1')
	req, err = http.NewRequest("GET", fmt.Sprintf("%s?q=%s:%s", environment.IndexSearchBaseUri, "@type", "Submission"), nil)
	assert.Nil(t, err)
	res, err = httpClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, 200, res.StatusCode)
	buf = bytes.Buffer{}
	defer res.Body.Close()
	io.Copy(&buf, res.Body)
	sHits := struct {
		Hits struct {
			Total int
			Hits  []struct {
				Source struct {
					Id          string `json:"@id"`
					Submitter   string
					Publication string
				} `json:"_source"`
			}
		}
	}{}
	json.Unmarshal(buf.Bytes(), &sHits)
	assert.True(t, sHits.Hits.Total > 1)
	found := 0
	for _, hit := range sHits.Hits.Hits {
		if hit.Source.Publication != "" && hit.Source.Submitter != "" {
			suri := hit.Source.Id
			req, err = newFedoraRequest("GET", suri, nil, "application/n-triples")
			assert.Nil(t, err)
			res, err = httpClient.Do(req)
			assert.Nil(t, err)
			defer res.Body.Close()
			buf.Reset()
			io.Copy(&buf, res.Body)

			if found == 0 {
				sUri = suri
				s, err = rdf.NewTripleDecoder(bytes.NewReader(buf.Bytes()), rdf.NTriples).DecodeAll()
				assert.Nil(t, err)
				found++
				continue
			}

			if found >= 0 {
				break
			}
		}
	}
	assert.True(t, len(sUri) > 0)
	assert.True(t, len(s) > 0)

	//   2a. Update 's' 'psub1' to reference any of the static publications in the repository (by default it points to a
	//      production PASS uri)
	//   2b. While we're at it, capture 'psub2' for use in step 3.
	req, err = http.NewRequest("GET", fmt.Sprintf("%s?q=%s:%s+%s:%s", environment.IndexSearchBaseUri, "@type", "Publication", "_exists_", "doi"), nil)
	assert.Nil(t, err)
	res, err = httpClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, 200, res.StatusCode)
	buf = bytes.Buffer{}
	defer res.Body.Close()
	io.Copy(&buf, res.Body)
	pHits := struct {
		Hits struct {
			Total int
			Hits  []struct {
				Source struct {
					Id string `json:"@id"`
				} `json:"_source"`
			}
		}
	}{}
	json.Unmarshal(buf.Bytes(), &pHits)
	assert.True(t, pHits.Hits.Total > 1)
	found = 0
	for _, hit := range pHits.Hits.Hits {
		if hit.Source.Id != "" && found == 0 {
			psub1 = hit.Source.Id
			found++
			continue
		}
		if hit.Source.Id != "" && found == 1 {
			psub2 = hit.Source.Id
		}

		if found >= 1 {
			break
		}
	}
	assert.True(t, len(psub1) > 0)
	assert.True(t, len(psub2) > 0)
	require.NotEqual(t, psub1, psub2)

	s = copyTriples(s, compositeTransformer(
		passTypeFilter(),
		passPredicateAndObjectFilter(),
		publicationTransformer("*", psub1)))

	//   3a. Copy 's' (call it s'), and update s' to reference a *different* static publication in the repository (call
	//       it pubsub2).
	//   3b. Create a *new* resource in the repository from s'

	sprimeUri = fmt.Sprintf("%s/%s/%s", environment.FcrepoBaseUri, "submissions", "dupeSubmission")
	sprime = copyTriples(s, compositeTransformer(
		passTypeFilter(),
		passPredicateAndObjectFilter(),
		subjectTransformer("*", sprimeUri),
		publicationTransformer("*", psub2)))

	// Replace the repository version of s with the transformed version
	buf.Reset()
	w := bufio.NewWriter(&buf)
	encoder := rdf.NewTripleEncoder(w, rdf.NTriples)
	encoder.EncodeAll(s)
	encoder.Close()
	err = replaceFedoraResource(t, environment, sUri, buf.Bytes(), "application/n-triples")
	assert.Nil(t, err)
	log.Printf("Replaced the content of %s", sUri)

	// Create a new repository resource based on sprime
	buf.Reset()
	w = bufio.NewWriter(&buf)
	encoder = rdf.NewTripleEncoder(w, rdf.NTriples)
	encoder.EncodeAll(sprime)
	encoder.Close()
	err = replaceFedoraResource(t, environment, sprimeUri, buf.Bytes(), "application/n-triples")
	assert.Nil(t, err)
	log.Printf("Replaced the content of %s", sprimeUri)

	// After doing this, the repository should contain:
	//   1. s referencing pubsub1
	//   2. s' referencing pubsub2
	//   3. where pubsub1 is a duplicate of pubsub2

	// Next, visit the repository, marking duplicates.  Users and publications must be marked first, then submissions.
	findDuplicatePublicationsAndUsers(t)

	queryPlan := query.NewPlanDecoder(sharedStore).Decode(queryPlanAllTheRest)[model.PassTypeSubmission]
	result := duplicateTestResult{dupes: make(map[string]int)}

	findDuplicate(t, &result, sharedStore, queryPlan, *typeContainers.get("Submission"), nil, nil)

	assert.True(t, result.executed) // that we executed the handler - and its assertions therein - supplied to the queryPlan at least once
	assert.Equal(t, 14, result.times)
	assert.Equal(t, 2, len(result.dupes))

	// visit the repository, marking duplicate users and publications
	// visit the repository, marking duplicate submissions
	// verify the count of duplicate submissions

	cleanup()
}

func findDuplicatePublicationsAndUsers(t *testing.T) {
	if err != nil {
		panic(err.Error())
	}

	plan := query.NewPlanDecoder(sharedStore).Decode(queryPlanPubsAndUsers)
	//log.Printf("Query plan: %s", plan)

	// store candidate duplicate uris and their type in the database
	handlerExecuted := false
	potentialDuplicates := map[string]int{}
	times := 0
	matchHandler := matchHandler(t, &handlerExecuted, &times, &potentialDuplicates, sharedStore)

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
	if err != nil {
		panic(err.Error())
	}

	plan := query.NewPlanDecoder(sharedStore).Decode(queryPlanAllTheRest)
	//log.Printf("Query plan: %s", plan)

	// store candidate duplicate uris and their type in the database
	handlerExecuted := false
	potentialDuplicates := map[string]int{}
	times := 0
	matchHandler := matchHandler(t, &handlerExecuted, &times, &potentialDuplicates, sharedStore)

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
	queryPlan := query.NewPlanDecoder(sharedStore).Decode(queryPlanAllTheRest)[model.PassTypeSubmission]
	result := duplicateTestResult{dupes: make(map[string]int)}

	findDuplicate(t, &result, sharedStore, queryPlan, *typeContainers.get("Submission"), nil, nil)

	assert.True(t, result.executed) // that we executed the handler - and its assertions therein - supplied to the queryPlan at least once
	assert.Equal(t, 4, result.times)
	assert.Equal(t, 3, len(result.dupes)) // for the two duplicate User resources
}

func findDuplicateUser(t *testing.T) {
	t.Parallel()
	queryPlan := query.NewPlanDecoder(sharedStore).Decode(queryPlanPubsAndUsers)[model.PassTypeUser]
	result := duplicateTestResult{dupes: make(map[string]int)}

	findDuplicate(t, &result, sharedStore, queryPlan, *typeContainers.get("User"), nil, nil)

	assert.True(t, result.executed) // that we executed the handler - and its assertions therein - supplied to the queryPlan at least once
	assert.Equal(t, 2, result.times)
	assert.Equal(t, 2, len(result.dupes)) // for the two duplicate User resources
}

func findDuplicateRepoCopy(t *testing.T) {
	t.Parallel()
	queryPlan := query.NewPlanDecoder(sharedStore).Decode(queryPlanAllTheRest)[model.PassTypeRepositoryCopy]
	result := duplicateTestResult{dupes: make(map[string]int)}

	findDuplicate(t, &result, sharedStore, queryPlan, *typeContainers.get("RepositoryCopy"), nil, nil)

	assert.True(t, result.executed)       // that we executed the handler - and its assertions therein - supplied to the queryPlan at least once
	assert.Equal(t, 4, result.times)      // one query for each resource plus an additional query for the resource with both the url and the publication
	assert.Equal(t, 3, len(result.dupes)) // for the three duplicate RepoCopy resources
}

func findDuplicateGrant(t *testing.T) {
	t.Parallel()
	queryPlan := query.NewPlanDecoder(sharedStore).Decode(queryPlanAllTheRest)[model.PassTypeGrant]
	result := duplicateTestResult{dupes: map[string]int{}}

	findDuplicate(t, &result, sharedStore, queryPlan, *typeContainers.get("Grant"), nil, nil)

	assert.True(t, result.executed) // that we executed the handler - and its assertions therein - supplied to the queryPlan at least once
	assert.Equal(t, 2, result.times)
	assert.Equal(t, 2, len(result.dupes)) // for the two duplicate Funder resources
}

func findDuplicateFunder(t *testing.T) {
	t.Parallel()
	queryPlan := query.NewPlanDecoder(sharedStore).Decode(queryPlanAllTheRest)[model.PassTypeFunder]
	result := duplicateTestResult{dupes: make(map[string]int)}

	findDuplicate(t, &result, sharedStore, queryPlan, *typeContainers.get("Funder"), nil, nil)

	assert.True(t, result.executed) // that we executed the handler - and its assertions therein - supplied to the queryPlan at least once
	assert.Equal(t, 2, result.times)
	assert.Equal(t, 2, len(result.dupes)) // for the two duplicate Funder resources
}

func findDuplicatePublication(t *testing.T) {
	t.Parallel()
	queryPlan := query.NewPlanDecoder(sharedStore).Decode(queryPlanPubsAndUsers)[model.PassTypePublication]
	result := duplicateTestResult{dupes: make(map[string]int)}

	findDuplicate(t, &result, sharedStore, queryPlan, *typeContainers.get("Publication"), nil, nil)

	assert.True(t, result.executed)       // that we executed the handler - and its assertions therein - supplied to the queryPlan at least once
	assert.Equal(t, 6, result.times)      // 1 query each for four resources, plus a two additional queries for the resource with all three properties
	assert.Equal(t, 4, len(result.dupes)) // for the four duplicate Publication resources
}

func findDuplicateJournalSimple(t *testing.T) {
	t.Parallel()
	journalQueryPlan := query.NewPlanDecoder(sharedStore).Decode(queryPlanSimpleJournal)[model.PassTypeJournal]
	result := duplicateTestResult{dupes: make(map[string]int)}

	findDuplicate(t, &result, sharedStore, journalQueryPlan, *typeContainers.get("Journal"), nil, nil)

	assert.True(t, result.executed)  // that we executed the handler - and its assertions therein - supplied to the journalQueryPlan at least once
	assert.Equal(t, 2, result.times) // for the two Journal resources that contain the 'nlmta' key (the third Journal resource does not)
	assert.Equal(t, 2, len(result.dupes))
}

func findDuplicateJournal(t *testing.T) {
	t.Parallel()
	journalQueryPlan := query.NewPlanDecoder(sharedStore).Decode(queryPlanAllTheRest)[model.PassTypeJournal]
	result := duplicateTestResult{dupes: make(map[string]int)}

	findDuplicate(t, &result, sharedStore, journalQueryPlan, *typeContainers.get("Journal"), nil, nil)

	assert.True(t, result.executed)       // that we executed the handler - and its assertions therein - supplied to the journalQueryPlan at least once
	assert.Equal(t, 3, len(result.dupes)) // we expect three potential duplicates; the journal with the single ISSN won't be found because we exact match on ISSNs, this is a TODO/FIXME
	assert.Equal(t, 5, result.times)      // the match handler executed once for each query that was performed.
}

func findDuplicate(t *testing.T, dtr *duplicateTestResult, store *persistence.Store, plan query.Plan, passType passType,
	containerHandler visit.ContainerHandler, eventHandler visit.EventHandler) {
	matchHandler := matchHandler(t, &dtr.executed, &dtr.times, &dtr.dupes, store)
	startUri := fmt.Sprintf("%s/%s", environment.FcrepoBaseUri, passType.containerName)
	passTypeUri := passType.typeUri
	executeQueryPlan(t, plan, startUri, passTypeUri, matchHandler, nil, nil)
}

func craftDuplicateSubmissionWithDuplicatePublications(t *testing.T) {
	var req *http.Request
	var res *http.Response
	var err error

	// We use PUT, so we know a priori the URI of the duplicate submission this method creates
	submissionCopyUri := fmt.Sprintf("%s/%s/%s", environment.FcrepoBaseUri, "submissions", "copiedSubmission")
	req, err = http.NewRequest("HEAD", submissionCopyUri, nil)
	req.SetBasicAuth(environment.FcrepoUser, environment.FcrepoPassword)
	assert.Nil(t, err)
	res, err = httpClient.Do(req)
	assert.Nil(t, err)
	defer func() { res.Body.Close() }()
	if res.StatusCode == 200 {
		return
	} else if res.StatusCode != 404 {
		assert.Failf(t, "Unexpected status code for HEAD %s, %d", submissionCopyUri, res.StatusCode)
	}

	// Check to see if that duplicate submission is already there (e.g. this method is being executed for a second time
	// against a populated repository), and if so, simply return.

	// Create a copy of any publication in the repository.
	pubSource := resources.get("publications")[0]
	pubTarget := copyFedoraResource(t, environment, pubSource, fmt.Sprintf("%s/%s/%s", environment.FcrepoBaseUri, "publications", "copiedPub"), noopTripleTransformer)
	log.Printf("Copied %s to %s", pubSource, pubTarget)

	// Find a a submission that has a 'publication' predicate, copy that submission, and update the copy of the submission
	// to point to the publication copy.

	// Find a Submission with a 'publication' property
	esQueryUrl := fmt.Sprintf("%s?q=publication:%s", environment.IndexSearchBaseUri, query.UrlQueryEscFunc(pubTarget))
	req, err = http.NewRequest("GET", esQueryUrl, nil)
	assert.Nil(t, err)
	res, err = httpClient.Do(req)
	assert.Nil(t, err)
	defer func() { res.Body.Close() }()
	buf := bytes.Buffer{}
	io.Copy(&buf, res.Body)
	hits := &struct {
		Hits struct {
			Total int
			Hits  []struct {
				Source map[string]interface{} `json:"_source"`
			}
		}
	}{}
	err = json.Unmarshal(buf.Bytes(), hits)
	assert.Nil(t, err)
	assert.True(t, hits.Hits.Total > 0)
	sourceSubmission := hits.Hits.Hits[0].Source["@id"].(string)

	// Copy that Submission, updating it to reference the copied publication
	targetSubmission := copyFedoraResource(t, environment, sourceSubmission, submissionCopyUri,
		publicationTransformer("*", pubTarget))
	log.Printf("Copied %s to %s", sourceSubmission, targetSubmission)

	// Update the original submission (the submission that was the source of the copied submission) to reference
	// the original publication

	req, err = http.NewRequest("GET", sourceSubmission, nil)
	req.SetBasicAuth(environment.FcrepoUser, environment.FcrepoPassword)
	req.Header.Add("Accept", "application/n-triples")
	res, err = httpClient.Do(req)
	defer res.Body.Close()
	buf.Reset()
	io.Copy(&buf, res.Body)
	triples, _ := rdf.NewTripleDecoder(bytes.NewReader(buf.Bytes()), rdf.NTriples).DecodeAll()
	var filteredTriples []rdf.Triple

	for _, triple := range triples {
		if strings.HasPrefix((triple).Pred.String(), model.PassResourceUriPrefix) ||
			strings.HasPrefix((triple).Obj.String(), model.PassResourceUriPrefix) {
			if fmt.Sprintf("%s%s", model.PassResourceUriPrefix, "publication") == (triple).Pred.String() {
				pubUri, _ := rdf.NewIRI(pubSource)
				filteredTriples = append(filteredTriples, rdf.Triple{(triple).Subj, (triple).Pred, pubUri})
			} else {
				filteredTriples = append(filteredTriples, triple)
			}
		}
	}

	buf.Reset()
	enc := rdf.NewTripleEncoder(bufio.NewWriter(&buf), rdf.NTriples)
	_ = enc.EncodeAll(filteredTriples)
	_ = enc.Close()

	if err := replaceFedoraResource(t, environment, sourceSubmission, buf.Bytes(), "application/n-triples"); err != nil {
		log.Fatalf("Error replacing resource %s: %s", sourceSubmission, err)
	}
}

var defaultContainerHandler = func(t *testing.T, queryPlan query.Plan, passType string, matchHandler query.MatchHandler) func(c model.LdpContainer) {
	return func(c model.LdpContainer) {
		log.Printf(">> Container: %s (%s)", c.Uri(), c.PassType())
		if isPass, candidate := c.IsPassResource(); isPass && candidate == passType {
			// note that if the container URI has been flagged as a duplicate in a previous invocation, then this
			// invocation is redundant
			if _, err := queryPlan.Execute(c, matchHandler); err != nil {
				switch {
				// allow for errors where keys cannot be extracted, this is to be expected with our tests
				case errors.Is(err, query.ErrMissingRequiredKey):
				// allow for errors where there may be a constraint violation; this is a consequence of using a shared
				// persistence store and running tests in parallel.
				case errors.Is(err, persistence.ErrConstraint):
				default:
					assert.Failf(t, "Error performing query: %s", err.Error())
				}
			}
		}
	}
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
			for candidateDupe := range localDuplicatesMap {
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

func Test_Cleanup(t *testing.T) {
	cleanup()
}
