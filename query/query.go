package query

import (
	"dupe-checker/model"
	"fmt"
	"net/http"
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
	Execute(container model.LdpContainer, handler func(result interface{}) (bool, error)) error
	Children() []Plan
}

type Config interface {
	Types() []string
	QueryPlan(resourceType string) PlanBuilder
}

type Match struct {
	// The elastic search query url used to perform the query
	QueryUrl string
	// The number of hits in the result, should be 1.  A count greater than 1 indicates a suspected duplicate
	HitCount int
	// This is the URI of the resource that is being searched for duplicates
	PassUri string
	// This is the type of the PASS resource that is being searched for duplicates
	PassType string
	// These are the URIs of potential matches, and the number of potential matches should equal the number of hits.
	// The PassUri is expected to be present in this slice
	MatchingUris []string
}
