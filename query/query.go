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

package query

import (
	"dupe-checker/model"
	"fmt"
	"net/http"
	"strings"
	"time"
)

const (
	// Noop QueryOp means that there is a single query template with no associated boolean logic applied to it
	Noop QueryOp = iota
	Or
	And
)

type Error struct {
	wrapped error
	context string
}

func (e Error) Unwrap() error {
	if e.wrapped != nil {
		return e.wrapped
	}

	return nil
}

func (e Error) Error() string {
	if e.wrapped == nil {
		return fmt.Sprintf("%s", e.context)
	}
	return fmt.Sprintf("%s: %s", e.wrapped.Error(), e.context)
}

type ElasticSearchClient struct {
	http http.Client
}

type QueryOp int

// Encapsulates an array of query templates, with an operator indicating how their results should be evaluated after the
// queries are executed.
type Operation struct {
	Op        QueryOp
	Templates []Template
}

type queryTemplateObj struct {
	Keys  []string `json:"keys"`
	Query string   `json:"q"`
}

type queryObj struct {
	Op        string
	Templates []queryTemplateObj
}

type Builder interface {
	Build() (Plan, error)
}

// Used to build a query plan for execution
type PlanBuilder interface {
	Builder
	Plan

	// Results should be boolean ORed
	// If Or() is invoked consecutively, that represents a nested "or" in the config:
	//   "or":   [
	//        {
	//          "or": [
	//            {
	//              ....
	//            },
	//            {
	//              ...
	//            }
	//          ]
	//        },
	//        {
	//          "keys": [
	//            "title"
	//          ],
	//          "q": "es query for title"
	//        }
	//      ]
	//    }
	//
	// If consecutive calls to Or() occur, a nested Plan will be created.
	Or() PlanBuilder
}

type TemplateBuilder interface {
	Builder
	AddKey(key string) TemplateBuilder
	AddQuery(query string) TemplateBuilder
	//AddPlan(p Plan) Plan
}

type Plan interface {
	// Executes the query Plan for the supplied container.  The handler accepts the result, and determines if processing
	// should be short-circuited by returning true.  The Execute method returns true if processing should be
	// short-circuited.
	Execute(container model.LdpContainer, handler func(result interface{}) (bool, error)) (bool, error)
	Children() []Plan
}

type Config interface {
	Types() []string
	QueryPlan(resourceType string) PlanBuilder
}

type Match struct {
	// The "front-porch" base URI which will be used in the PassUri.
	fedoraBaseUri string
	// The "back-porch" base URI which will be used in the MatchingUris.
	indexBaseUri string
	// The elastic search query url used to perform the query
	QueryUrl string
	// The number of hits in the result, should be 1.  A count greater than 1 indicates a suspected duplicate
	HitCount int
	// This is the URI of the resource that is being searched for duplicates
	// It originates from the model.LdpContainer.Uri() (i.e., the so-called "front porch URI")
	PassUri string
	// This is the type of the PASS resource that is being searched for duplicates
	PassType string
	// These are the URIs of potential matches, and the number of potential matches should equal the number of hits.
	// Matching URIs are populated by Elastic Search results, and their scheme, hostname, and port may differ from that
	// used in the PassUri.  Elastic Search contains the so-called "back porch URI".
	MatchingUris []string
	// These are the fields of the PASS resource that were matched by the query
	MatchFields []string
	// These are the values of the fields that were matched by the query, keyed by MatchingUris
	MatchValues map[string][]string
	// Properties of the container that may be useful for auditing
	ContainerProperties struct {
		SourceCreatedBy,
		SourceLastModifiedBy string
		SourceCreated,
		SourceLastModified time.Time
	}
}

// MatchHandler accepts the result from an Elastic Search query, i.e. a 'Match' (represented as an interface{} in
// the method signature) and does something with it.  A typical implementation will determine if the query found
// duplicate resources (HitCount > 1, and PassUri != any of the MatchingUris), and store those duplicates in the
// database.
//
// A MatchHandler can return 'true' if it believes no more queries need to be executed, otherwise it should return
// false.  Any errors that cannot be handled gracefully should be returned.
type MatchHandler func(result interface{}) (shortCircuit bool, err error)

// Strips the back and front porch Fedora base URIs from the supplied URIs, and compares the remaining URI paths for
// equality.  The front and back porch base URIs are kept as internal state, initialized when the Match is created.
// Typically the front and back porch base URIs come from the environment.
//
// If the necessary internal state is not present on Match, this method panics.
//
// If a supplied URI does not contain either prefix, the entire URI will be used for comparison
func (m *Match) UriPathsEqual(a, b string) bool {
	if strings.TrimSpace(m.fedoraBaseUri) == "" || strings.TrimSpace(m.indexBaseUri) == "" {
		panic("cannot compare URI paths, as the base uri to strip are not set")
	}
	a = m.StripBaseUri(a)
	b = m.StripBaseUri(b)
	return a == b
}

// Strips the back and front porch Fedora base URIs from the supplied URI, and returns the remaining URL path.  The
// front and back porch base URIs are kept as internal state, initialized when the Match is created. Typically the front
// and back porch base URIs come from the environment.
//
// If the necessary internal state is not present on Match, this method panics.
//
// If a supplied URI does not contain either prefix, the entire URI will be used for comparison
func (m *Match) StripBaseUri(uri string) string {
	if strings.TrimSpace(m.fedoraBaseUri) == "" || strings.TrimSpace(m.indexBaseUri) == "" {
		panic("cannot compare URI paths, as the base uri to strip are not set")
	}
	return strip(uri, m.fedoraBaseUri, m.indexBaseUri)
}

// trims the prefixes from the supplied string
func strip(string string, prefix ...string) string {
	for _, p := range prefix {
		if strings.HasPrefix(string, p) {
			string = strings.TrimPrefix(string, p)
		}
	}
	return string
}
