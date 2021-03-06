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

package main

import (
	"dupe-checker/env"
	"dupe-checker/persistence"
	"dupe-checker/process"
	"dupe-checker/query"
	"dupe-checker/retrieve"
	"dupe-checker/visit"
	_ "embed"
	"errors"
	"flag"
	"fmt"
	"github.com/mattn/go-sqlite3"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

//go:embed internal/integration/queryplan-publicationsandusers.json
var queryPlanUsersAndPubs string

//go:embed internal/integration/queryplan-alltherest.json
var queryPlanAllTheRest string

// Usage: ./passrdc -starturi <base repository uri> -indexuri <base elasticsearch uri> -dsn <sqlite dsn>
// SQLITE_DSN file:/tmp/dupechecker.db
// HTTP_TIMEOUT_MS 600000
// FCREPO_BASE_URI http://fcrepo:8080/fcrepo/rest
// FCREPO_USER fedoraAdmin
// FCREPO_PASS
// FCREPO_MAX_CONCURRENT_REQUESTS 5
// INDEX_SEARCH_BASE_URI http://elasticsearch:9200/pass/_search
func main() {

	environment := env.New()

	fs := flag.NewFlagSet("passrdc", flag.ExitOnError)
	fs.Usage = func() {
		os.Stdout.WriteString(fmt.Sprintf("Usage: %s -starturi <base repository uri> -indexuri <base elasticsearch uri> -dsn <sqlite dsn>\n", os.Args[0]))
		os.Stdout.WriteString(fmt.Sprintf("  example: %s -starturi %s -indexuri %s -dsn %s\n", os.Args[0], "http://fcrepo:8080/fcrepo/rest", "http://elasticsearch:9200/pass/_search", "file:/passrdc.db?mode=rwc"))
	}

	startUri := fs.String("starturi", environment.FcrepoBaseUri, "the uri of the PASS container to check for duplicates")
	indexUri := fs.String("indexuri", environment.IndexSearchBaseUri, "the uri of the Elastic Search index search endpoint")
	dsn := fs.String("dsn", environment.SqliteDsn, "the DSN of the Sqlite db used to store results")

	checkAndAssignArgs(fs, startUri, indexUri, dsn)

	// re-create environment as checkAndAssignArgs will set env vars.
	environment = env.New()

	var err error
	var store persistence.Store
	var visitor *visit.Controller

	store, err = newStore(*dsn)
	if err != nil {
		log.Fatalf("error obtaining sqlite store: %s", err)
	}

	visitor = createVisitor(environment)

	matchHandler := func(result interface{}) (bool, error) {
		candidateDupes := map[string]int{}
		match := result.(query.Match)
		for _, matchingUri := range match.MatchingUris {
			if match.UriPathsEqual(match.PassUri, matchingUri) {
				continue
			}
			if _, contains := candidateDupes[matchingUri]; contains {
				candidateDupes[matchingUri]++
			} else {
				candidateDupes[matchingUri] = 1
			}
		}

		for candidateDupe, _ := range candidateDupes {
			if err := store.StoreDupe(match.StripBaseUri(match.PassUri), match.StripBaseUri(candidateDupe), match.PassType, match.MatchFields, persistence.DupeContainerAttributes{
				SourceCreatedBy:      match.ContainerProperties.SourceCreatedBy,
				SourceCreated:        match.ContainerProperties.SourceCreated,
				SourceLastModifiedBy: match.ContainerProperties.SourceLastModifiedBy,
				SourceLastModified:   match.ContainerProperties.SourceLastModified,
			}); err != nil && !errors.Is(err, persistence.ErrConstraint) {
				panic(fmt.Sprintf("%s", err))
			}
		}

		return false, nil
	}

	uberPlan := mergePlans(decodeQueryPlan(queryPlanUsersAndPubs), decodeQueryPlan(queryPlanAllTheRest))
	p := process.New(uberPlan, matchHandler)

	log.Printf("Beginning dupe check of %s, using index %s and database dsn %s", *startUri, *indexUri, *dsn)
	log.Printf("HTTP timeout (ms) (%s): %s", env.HTTP_TIMEOUT_MS, environment.HttpTimeoutMs)
	log.Printf("Fedora base URI (%s): %s", env.FCREPO_BASE_URI, environment.FcrepoBaseUri)
	log.Printf("Index base URI (%s): %s", env.FCREPO_INDEX_BASE_URI, environment.FcrepoIndexBaseUri)
	log.Printf("Fedora user (%s): %s", env.FCREPO_USER, environment.FcrepoUser)
	log.Printf("Max concurrent requests (to Fedora) (%s): %s", env.FCREPO_MAX_CONCURRENT_REQUESTS, environment.FcrepoMaxConcurrentRequests)

	p.Process(*startUri, visitor, environment)
	log.Printf("Dupe check complete; review analysis in the sqlite database at %s", *dsn)
}

// TODO: annoying - fix this by just combining everything into a single JSON document and update ITs to deal with it.
func mergePlans(a, b map[string]query.Plan) map[string]query.Plan {
	for passType := range a {
		if _, exists := b[passType]; exists {
			log.Fatalf("Plan for %s already exists", passType)
		} else {
			b[passType] = a[passType]
		}
	}
	return b
}

func createVisitor(environment env.Env) *visit.Controller {
	if timeout, err := strconv.Atoi(environment.HttpTimeoutMs); err != nil || timeout < 1 {
		log.Fatalf("error parsing the value of env var %s, must be a positive integer: %s", env.HTTP_TIMEOUT_MS, err)
	} else {
		httpClient := http.Client{Timeout: time.Duration(timeout) * time.Millisecond}
		retriever := newRetriever(httpClient, environment.FcrepoUser, environment.FcrepoPassword, "pass-rdc/0.0.1")
		if maxreqs, err := strconv.Atoi(environment.FcrepoMaxConcurrentRequests); err != nil || maxreqs < 1 {
			log.Fatalf("error parsing the value of env var %s, must be a positive integer: %s", env.FCREPO_MAX_CONCURRENT_REQUESTS, err)
		} else {
			v := newVisitor(retriever, maxreqs)
			return &v
		}
	}
	return nil
}

func checkAndAssignArgs(fs *flag.FlagSet, startUri *string, indexUri *string, dsn *string) {
	if err := fs.Parse(os.Args[1:]); err != nil {
		log.Printf("Error parsing command line args: %s", err.Error())
		os.Exit(1)
	}

	if err := handleFcrepoBaseUri(*startUri); err != nil {
		log.Printf("%s", err.Error())
		os.Exit(1)
	}

	if err := handleIndexUri(*indexUri); err != nil {
		log.Printf("%s", err.Error())
		os.Exit(1)
	}

	if err := handleSqliteDsn(*dsn); err != nil {
		log.Printf("%s", err.Error())
		os.Exit(1)
	}
}

// if FCREPO_BASE_URI is not set, derive it from the startUri and set it.
// if FCREPO_BASE_URI is set, insure that startUri can be derived from it
// (i.e. the startUri should be the same as the base uri or subordinate to it)
func handleFcrepoBaseUri(cliVal string) error {
	cliVal = strings.TrimSpace(cliVal)

	if cliVal == "" {
		return errors.New("starturi is required")
	}

	if _, err := url.Parse(cliVal); err != nil {
		return err
	}

	defaultBasePath := "/fcrepo/rest"
	var envVal string
	var exists bool

	envVal, exists = os.LookupEnv(env.FCREPO_BASE_URI)

	if !exists {
		if i := strings.Index(cliVal, defaultBasePath); i > -1 {
			baseUri := cliVal[0 : i+len(defaultBasePath)]
			if err := os.Setenv(env.FCREPO_BASE_URI, baseUri); err != nil {
				return errors.New(fmt.Sprintf("unable to set env var %s to %s", env.FCREPO_BASE_URI, baseUri))
			}
		} else {
			// we don't know if the cliVal is subordinate to the FCREPO_BASE_URI or if it *is* the FCREPO_BASE_URI.
			// The user needs to set it.
			return errors.New(fmt.Sprintf("unable to determine the FCREPO_BASE_URI from %s, please set and export the %s environment variable and retry", cliVal, env.FCREPO_BASE_URI))
		}
	} else {
		if !strings.HasPrefix(cliVal, envVal) {
			return errors.New(fmt.Sprintf("-starturi value %s must be equal or subordinate to %s %s", cliVal, env.FCREPO_BASE_URI, envVal))
		}
	}

	return nil
}

// if SQLITE_DSN is not set, derive it from the dsn and set it.
// if SQLITE_DSN is set, overwrite it.
// i.e. if the DSN is provided on the CLI it overrides any existing env var.
func handleSqliteDsn(cliVal string) error {
	//var envVal string
	//var exists bool

	if strings.TrimSpace(cliVal) != "" {
		return os.Setenv(env.SQLITE_DSN, cliVal)
	}

	if _, exists := os.LookupEnv(env.SQLITE_DSN); !exists {
		return errors.New(fmt.Sprintf("unable to determine the Sqlite DSN from the command line (-dsn) or the environment (%s), either provide a value for -dsn on the command line, or set the env var %s", env.SQLITE_DSN, env.SQLITE_DSN))
	}

	return nil
}

func handleIndexUri(cliVal string) error {
	cliVal = strings.TrimSpace(cliVal)

	if cliVal != "" {
		if _, err := url.Parse(cliVal); err != nil {
			return err
		}
		return os.Setenv(env.INDEX_SEARCH_BASE_URI, cliVal)
	}

	if _, exists := os.LookupEnv(env.INDEX_SEARCH_BASE_URI); !exists {
		return errors.New(fmt.Sprintf("unable to determine the Index URI from the command line (-indexuri) or the environment (%s), either provide a value for -indexuri on the command line, or set the env var %s", env.INDEX_SEARCH_BASE_URI, env.INDEX_SEARCH_BASE_URI))
	} else {
		if _, err := url.Parse(cliVal); err != nil {
			return err
		}
	}

	return nil
}

// Answers the persistence store used to save the progress and analysis of the deduplication effort
func newStore(dsn string) (persistence.Store, error) {
	// TODO: support obtaining connection params from env
	s, e := persistence.NewSqlLiteStore(dsn, persistence.SqliteParams{
		MaxIdleConn: 4,
		MaxOpenConn: 4,
	}, nil)

	return s, e
	if e != nil {
		return s, e
	}

	retryStore := persistence.NewRetrySqliteStore(s, 500*time.Millisecond, 1.5, 3,
		sqlite3.ErrBusy, sqlite3.ErrLocked, sqlite3.ErrConstraint)
	return retryStore, nil
}

// Answers the query plan, one Plan per PASS type.
// Keys are full RDF URIs representing PASS types, e.g. http://oapass.org/ns/pass#Submission.
func decodeQueryPlan(planJson string) map[string]query.Plan {
	return query.NewPlanDecoder().Decode(planJson)
}

// Answers a Retriever implementation, used to get resources from the PASS repository
func newRetriever(client http.Client, user, password, useragent string) retrieve.Retriever {
	return retrieve.New(&client, user, password, useragent)
}

// Answers a Controller for walking the resources of the Fedora repository
func newVisitor(r retrieve.Retriever, maxConcurrentReqs int) visit.Controller {
	return visit.NewController(r, maxConcurrentReqs)
}
