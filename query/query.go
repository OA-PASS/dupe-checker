package query

import "text/template"

const (
	Or QueryOp = iota
	And
)

type QueryOp int

// Associates a named key with a value; used when evaluating the template
type KvPair struct {
	key, value string
}

// Encapsulates an ES query and the keys it requires for evaluation
type Template struct {
	Template template.Template
	Keys     []string
}

// Parameterizes the template with supplied key-value pairs and returns the query, ready to be executed
func (qt Template) Eval(kvp []KvPair) (string, error) {
	return "", nil
}

// Template is also a Plan.
func (qt Template) Execute(handler func(result string) error) error {
	return nil
}

// Encapsulates an array of query templates, with an operator indicating how their results should be evaluated after the
// queries are executed.
type Operation struct {
	Op        QueryOp
	Templates []Template
}

// Used to build a query plan for execution
type PlanBuilder interface {
	// Results of the supplied QueryTemplates should be boolean ORed
	Or(queryOpr ...Plan) PlanBuilder

	// Results of the supplied QueryTemplates should be boolean ANDed
	And(queryOpr ...Plan) PlanBuilder

	// Joins QueryTemplates.  Illegal to supply a QueryTemplate if Or or And has been invoked previously.
	With(queryOpr ...Plan) PlanBuilder

	// Finalizes the state of the QueryPlanBuilder and readies it for execution
	Build() Plan
}

type Plan interface {
	Execute(handler func(result string) error) error
}

type Config interface {
	Types() []string
	QueryPlan(resourceType string) PlanBuilder
}

type Executor interface {
	Execute(query string, handler func(result string) error) error
}
