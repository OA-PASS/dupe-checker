package query

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"text/template"
)

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
	Or() TemplateBuilder

	// Results of the supplied QueryTemplates should be boolean ANDed
	//And() *TemplateBuilder

	// Joins QueryTemplates.  Illegal to supply a QueryTemplate if Or or And has been invoked previously.
	//With() *TemplateBuilder

	// Initializes the PlanBuilder for the specified resource type
	// Should only be invoked once for each type; i.e. one type, one Plan.
	//ForResource(rdfType string) PlanBuilder

	AddPlan(p Plan)
}

type TemplateBuilder interface {
	Builder
	AddKey(key string) TemplateBuilder
	AddQuery(query string) TemplateBuilder
	AddPlan(p Plan) Plan
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

type tmplBuilderImpl struct {
	built bool
	keys  []string
	query string
}

func newTmplBuilder() tmplBuilderImpl {
	return tmplBuilderImpl{}
}

func (tb *tmplBuilderImpl) AddKey(key string) TemplateBuilder {
	if tb.keys == nil {
		tb.keys = []string{key}
	} else {
		tb.keys = append(tb.keys, key)
	}

	return tb
}

func (tb *tmplBuilderImpl) AddQuery(query string) TemplateBuilder {
	if len(tb.query) == 0 {
		tb.query = query
	} else {
		log.Fatalf("illegal state: cannot overwrite existing query '%s' with query '%s'", tb.query, query)
	}

	return tb
}

func (tb *tmplBuilderImpl) AddPlan(p Plan) Plan {
	panic("implement me")
}

func (tb *tmplBuilderImpl) Build() (Plan, error) {
	if tb.built {
		return nil, errors.New("illegal state: this TemplateBuilder has already been built")
	}
	tb.built = true
	return tb, nil
}

func (tb *tmplBuilderImpl) String() string {
	return fmt.Sprintf("(%T) built: %t keys: %s q: %s\n", tb, tb.built, strings.Join(tb.keys, ","), tb.query)
}

func (tb *tmplBuilderImpl) Execute(handler func(result string) error) error {
	panic("implement me")
}

type planBuilderImpl struct {
	built            bool
	subordinate      *planBuilderImpl
	templateBuilders []*tmplBuilderImpl
}

func (pb planBuilderImpl) String() string {
	return pb.string(&strings.Builder{}, "", "")
}

func (pb planBuilderImpl) string(sb *strings.Builder, leadIndent, indent string) string {
	sb.WriteString(fmt.Sprintf("%s(%T) built: %t \n", leadIndent, pb, pb.built))

	if pb.subordinate != nil {
		sb.WriteString(fmt.Sprintf("%s  subordinate: ", indent))
		pb.subordinate.string(sb, "", indent+"  ")
	} else {
		sb.WriteString(fmt.Sprintf("%s  subordinate: nil", indent))
	}

	for _, v := range pb.templateBuilders {
		sb.WriteString(fmt.Sprintf("%s  templates:\n    %s", indent, v))
	}

	sb.WriteString("\n")

	return sb.String()
}

func newPlanBuilder() planBuilderImpl {
	return planBuilderImpl{}
}

func (pb *planBuilderImpl) addSubordinatePlanBuilder() *planBuilderImpl {
	if pb.subordinate != nil {
		log.Fatalf("illegal state: this PlanBuilder already has as subordinate PlanBuilder for recursive operations")
	}

	pb.subordinate = &planBuilderImpl{}
	return pb.subordinate
}

func (pb *planBuilderImpl) AddPlan(p Plan) {
	//if pb.plans == nil {
	//	pb.plans = []Plan{p}
	//} else {
	//	pb.plans = append(pb.plans, p)
	//}
}

func (pb planBuilderImpl) Execute(handler func(result string) error) error {
	panic("implement me")
}

func (pb *planBuilderImpl) Build() (Plan, error) {
	pb.built = true

	if pb.subordinate != nil {
		if _, err := pb.subordinate.Build(); err != nil {
			log.Fatal(err)
		}
	}

	return pb, nil
}

func (pb *planBuilderImpl) Or() TemplateBuilder {
	tmplBuilder := tmplBuilderImpl{}

	if pb.subordinate == nil {
		// first invocation of Or(), add the TemplateBuilder to *this* PlanBuilder
		pb.templateBuilders = append(pb.templateBuilders, &tmplBuilder)
		// add the subordinate PlanBuilder to handle a subsequent call to Or()
		pb.addSubordinatePlanBuilder()
	} else {
		// otherwise Or() has been invoked at least once on this PlanBuilder, so we add and return a TemplateBuilder
		// on the *subordinate* PlanBuilder
		pb.subordinate.templateBuilders = append(pb.subordinate.templateBuilders, &tmplBuilder)
	}

	return &tmplBuilder
}

/*
func (pb *planBuilderImpl) And() TemplateBuilder {
	return nil
	//return pb.addSubordinatePlanBuilder()
}

func (pb *planBuilderImpl) With() TemplateBuilder {
	panic("implement me")
}

func (pb *planBuilderImpl) ForResource(rdfType string) PlanBuilder {
	panic("implement me")
}

func (pb *planBuilderImpl) Execute(handler func(result string) error) error {
	panic("implement me")
}\
*/
