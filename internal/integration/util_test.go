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
	"github.com/knakk/rdf"
	"github.com/logrusorgru/aurora/v3"
	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// returns true if the current time minus the start time is greater than the timeout duration
func timedout(start time.Time, timeout time.Duration) bool {
	isTimedOut := time.Now().Sub(start) > timeout
	return isTimedOut
}

func perform(req *http.Request, assertStatusCode int) error {
	req.SetBasicAuth(environment.FcrepoUser, environment.FcrepoPassword)
	if res, err := httpClient.Do(req); err != nil {
		return err
	} else {
		defer res.Body.Close()
		bod := &bytes.Buffer{}
		io.Copy(bod, res.Body)
		if res.StatusCode != assertStatusCode {
			return errors.New("Error creating container, " + res.Status + ": " + bod.String())
		}
	}

	return nil
}

func performWithHook(req *http.Request, bodyHook func(statusCode int, body io.Reader) error) error {
	req.SetBasicAuth(environment.FcrepoUser, environment.FcrepoPassword)
	if res, err := httpClient.Do(req); err != nil {
		return err
	} else {
		defer res.Body.Close()
		return bodyHook(res.StatusCode, res.Body)
	}
}

func testResources(shellGlob string, embeddedFs embed.FS) []fs.DirEntry {
	var matches []fs.DirEntry

	if entries, err := embeddedFs.ReadDir("."); err != nil {
		log.Fatalf("Error listing PASS test resources: %s", err.Error())
	} else {
		for _, entry := range entries {
			if matched, _ := path.Match(shellGlob, entry.Name()); matched {
				matches = append(matches, entry)
			}
		}
	}

	return matches
}

// Fedora, ElasticSearch, ActiveMQ and the Indexer all need to be up.
// Verify tcp connectivity to dependencies
func checkDependentServices(serviceDeps *map[string]bool) {
	skipDeps, _ := strconv.ParseBool(environment.ItSkipServiceDepCheck)
	if !skipDeps {
		wg := sync.WaitGroup{}
		wg.Add(len(*serviceDeps))
		mu := sync.Mutex{}

		for hostAndPort := range *serviceDeps {
			go func(hostAndPort string) {
				timeout := 5 * time.Second
				start := time.Now()

				for !timedout(start, timeout) {
					fmt.Printf(aurora.Sprintf(aurora.Green("Dialing %v\n"), hostAndPort))
					if c, err := net.Dial("tcp", hostAndPort); err == nil {
						_ = c.Close()
						mu.Lock()
						(*serviceDeps)[hostAndPort] = true
						mu.Unlock()
						fmt.Print(aurora.Sprintf(aurora.Green("Successfully connected to %v\n"), hostAndPort))
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

		for k, v := range *serviceDeps {
			if !v {
				fmt.Printf(aurora.Sprintf(aurora.Red("failed to connect to %v"), k))
				os.Exit(-1)
			}
		}
	}
}

// Create parent containers, http://fcrepo:8080/fcrepo/rest/journals, http://fcrepo:8080/fcrepo/rest/users, etc.
// Populate them with test resources.
// If Fedora already has a container, skip the initialization of resources for that container.  Any pre-existing
// containers will not be deleted after.
func initializeContainersAndResources() {
	for _, typeContainer := range typeContainers {
		containerName := typeContainer.containerName
		url := fmt.Sprintf("%s/%s", environment.FcrepoBaseUri, containerName)
		req, _ := http.NewRequest("HEAD", url, nil)
		if err := perform(req, 200); err == nil {
			log.Println(aurora.Sprintf(aurora.Green("setup: container %s already exists.  Skipping initialization."), url))
			typeContainer.preExists = true
			req, _ = http.NewRequest("GET", url, nil)
			req.SetBasicAuth(environment.FcrepoUser, environment.FcrepoPassword)
			req.Header.Add("Accept", "application/n-triples")
			err = performWithHook(req, func(statusCode int, body io.Reader) error {
				if statusCode != 200 {
					return errors.New("Expected status code when retrieving " + url)
				}
				if trips, err := rdf.NewTripleDecoder(body, rdf.NTriples).DecodeAll(); err != nil {
					return err
				} else {
					for _, trip := range trips {
						if trip.Pred.String() != model.LdpContainsUri {
							continue
						}
						if entry, exists := resources[typeContainer]; exists {
							entry = append(entry, trip.Obj.String())
						} else {
							resources[typeContainer] = []string{trip.Obj.String()}
						}
					}
				}
				return nil
			})
			if err != nil {
				log.Fatalf("Error retrieving existing resources: %s", err)
			}
			continue
		}

		req, _ = http.NewRequest("PUT", url, nil)
		req.Header.Add("Content-Type", "application/n-triples")
		if err := perform(req, 201); err != nil {
			log.Fatalf("Error creating container %s: %s", url, err.Error())
		} else {
			log.Print(aurora.Sprintf(aurora.Green("setup: created container %s"), url))
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
			if err := performWithHook(req, func(statusCode int, body io.Reader) error {
				buf := &bytes.Buffer{}
				_, _ = io.Copy(buf, body)
				if statusCode != 201 {
					return errors.New(buf.String())
				}
				if entry, exists := resources[typeContainer]; exists {
					entry = append(entry, strings.TrimSpace(buf.String()))
				} else {
					resources[typeContainer] = []string{strings.TrimSpace(buf.String())}
				}
				return nil
			}); err != nil {
				log.Fatalf("Error creating repository resource under %s from %s: %s", url,
					testResource.Name(), err.Error())
			} else {
				log.Println(aurora.Sprintf(aurora.Green("setup: created test resource in repository under %s from %s"), url, testResource.Name()))
			}
		}
	}

	// give time for the indexer to process any newly created resources
	time.Sleep(2 * time.Second)
}

func createPersistenceStore() {
	var storeDsn string

	if isPreserveState() {
		if f, e := os.CreateTemp("", "passrdcit-*.db"); e != nil {
			log.Fatal(aurora.Sprintf(aurora.Red("error creating temporary file for database: %s"), e))
		} else {
			storeDsn = fmt.Sprintf("file:%s", f.Name())
			log.Println(aurora.Sprintf(aurora.Green("setup: preserving database state at %s per %s=%s"),
				f.Name(), env.IT_PRESERVE_STATE, environment.ItPreserveState))
		}
	} else {
		storeDsn = "file::memory:?cache=shared"
	}
	if store, err := persistence.NewSqlLiteStore(storeDsn, persistence.SqliteParams{
		MaxIdleConn: 4,
		MaxOpenConn: 4,
	}, nil); err != nil {
		log.Fatalf("Error creating persistence.Store: %s", err)
	} else {
		var retryIntervalMs int
		var maxTries int
		if retryIntervalMs, err = strconv.Atoi(environment.ItSqliteRetryIntervalMs); err != nil {
			log.Fatalf("Invalid value for %s, must be a positive integer (was '%v')",
				env.IT_SQLITE_RETRY_INTERVAL_MS, environment.ItSqliteRetryIntervalMs)
		}
		if maxTries, err = strconv.Atoi(environment.ItSqliteMaxRetry); err != nil {
			log.Fatalf("Invalid value for %s, must be a positive integer (was '%v')",
				env.IT_SQLITE_MAX_RETRY, environment.ItSqliteMaxRetry)
		}

		s := persistence.NewRetrySqliteStore(store, time.Duration(retryIntervalMs)*time.Millisecond, 1.2, maxTries, sqlite3.ErrLocked, sqlite3.ErrBusy)
		sharedStore = &s
		log.Println(aurora.Sprintf(aurora.Green("Initialized %T@%p"), sharedStore, sharedStore))
	}
}

func cleanup() {
	// Empty out the repository unless IT_PRESERVE_STATE is true, or if a container was previously initialized
	if !isPreserveState() {
		log.Println(aurora.Green("tear down: removing resources from Fedora"))
		var toDelete []passType
		for container := range resources {
			toDelete = append(toDelete, container)
		}
		for _, container := range toDelete {
			var req *http.Request
			req, err = http.NewRequest("DELETE", fmt.Sprintf("%s/%s", environment.FcrepoBaseUri, container.containerName), nil)
			req.SetBasicAuth(environment.FcrepoUser, environment.FcrepoPassword)
			_, err = httpClient.Do(req)
			req, err = http.NewRequest("DELETE", fmt.Sprintf("%s/%s/fcr:tombstone", environment.FcrepoBaseUri, container.containerName), nil)
			req.SetBasicAuth(environment.FcrepoUser, environment.FcrepoPassword)
			_, err = httpClient.Do(req)
			if err != nil {
				log.Fatal(aurora.Red(fmt.Sprintf("error cleaning up state: %s", err)))
			}
		}
	} else {
		log.Println(aurora.Sprintf(aurora.Green("tear down: preserving contents of Fedora per %s=%s"),
			env.IT_PRESERVE_STATE, environment.ItPreserveState))
	}
}

// Answers the value of the env.IT_PRESERVE_STATE environment variable.  If 'true', then the IT should attempt to
// preserve the state of the test after it completes.
//
// Normally env.IT_PRESERVE_STATE would only be set for the execution of a single test.
func isPreserveState() bool {
	if preserveState, err := strconv.ParseBool(environment.ItPreserveState); err != nil {
		log.Fatalf("Invalid value for %s: %s", env.IT_PRESERVE_STATE, environment.ItPreserveState)
	} else {
		return preserveState
	}

	return false
}

func executeQueryPlan(t *testing.T, queryPlan query.Plan, startUri string, passType string, matchHandler query.MatchHandler, containerHandler visit.ContainerHandler, eventHandler visit.EventHandler) {
	retriever := retrieve.New(&httpClient, environment.FcrepoUser, environment.FcrepoPassword, "test_findDuplicateJournal")
	maxReq, err := strconv.Atoi(environment.FcrepoMaxConcurrentRequests)
	assert.Nil(t, err)

	controller := visit.NewController(retriever, maxReq)
	controller.ErrorHandler(visit.NoopErrorHandler)
	if eventHandler == nil {
		controller.EventHandler(visit.NoopEventHandler)
	} else {
		controller.EventHandler(eventHandler)
	}
	if containerHandler == nil {
		controller.ContainerHandler(defaultContainerHandler(t, queryPlan, passType, matchHandler))
	} else {
		controller.ContainerHandler(containerHandler)
	}
	controller.Begin(startUri, visit.AcceptAllFilter, visit.AcceptAllFilter)
}

func newFedoraRequest(method, url string, body io.Reader, mediaType string) (*http.Request, error) {
	if req, err := http.NewRequest(method, url, body); err != nil {
		return nil, err
	} else {
		req.SetBasicAuth(environment.FcrepoUser, environment.FcrepoPassword)
		if mediaType != "" {
			header := ""
			switch strings.ToUpper(method) {
			case "GET":
				header = "Accept"
			case "POST", "PUT":
				header = "Content-Type"
			default:
				panic("Unhandled media type " + mediaType)
			}
			req.Header.Add(header, mediaType)
		}

		return req, nil
	}
}
