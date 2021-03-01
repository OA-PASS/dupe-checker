package query

import (
	"dupe-checker/model"
	"net/http"
)

const (
	// Noop QueryOp means that there is a single query template with no associated boolean logic applied to it
	Noop QueryOp = iota
	Or
	And
)

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
	Execute(container model.LdpContainer, handler func(result string) error) error
	Children() []Plan
}

type Config interface {
	Types() []string
	QueryPlan(resourceType string) PlanBuilder
}
