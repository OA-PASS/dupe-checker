package query

import (
	"errors"
	"fmt"
	"strings"
	"text/template"
)

const (
	// Noop QueryOp means that there is a single query template with no associated boolean logic applied to it
	Noop QueryOp = iota
	Or
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
	Execute(handler func(result string) error) error
	Children() []Plan
}

type Config interface {
	Types() []string
	QueryPlan(resourceType string) PlanBuilder
}

type planBuilderImpl struct {
	built     bool
	oper      QueryOp
	children  []*planBuilderImpl
	templates []*tmplBuilderImpl
	active    *tmplBuilderImpl
}

type tmplBuilderImpl struct {
	built bool
	keys  []string
	query string
}

func newTmplBuilder() tmplBuilderImpl {
	return tmplBuilderImpl{}
}

func (pb *planBuilderImpl) Children() []Plan {
	children := make([]Plan, len(pb.children)+len(pb.templates))
	for i := range pb.children {
		children[i] = pb.children[i]
	}
	for i := range pb.templates {
		children[len(pb.children)+i] = pb.templates[i]
	}
	return children
}

func (tb *tmplBuilderImpl) Children() []Plan {
	// query templates don't have children
	return []Plan{}
}

func (tb *tmplBuilderImpl) Or() PlanBuilder {
	panic("implement me")
}

func (tb *tmplBuilderImpl) ifBuilt(msg string, shouldPanic bool) error {
	if tb.built {
		if shouldPanic {
			panic(msg)
		} else {
			return errors.New(msg)
		}
	}

	return nil
}
func (tb *tmplBuilderImpl) AddKey(key string) TemplateBuilder {
	tb.ifBuilt(
		fmt.Sprintf("illegal state: cannot append key '%s' to existing keys '%s': already built %T@%p\n%s", key, strings.Join(tb.keys, ","), tb, tb, tb), true)

	if tb.keys == nil {
		tb.keys = []string{key}
	} else {
		tb.keys = append(tb.keys, key)
	}

	return tb
}

func (tb *tmplBuilderImpl) AddQuery(query string) TemplateBuilder {
	tb.ifBuilt(
		fmt.Sprintf("illegal state: cannot overwrite existing query '%s' with query '%s': already built %T@%p\n%s", tb.query, query, tb, tb, tb), true)

	if len(tb.query) == 0 {
		tb.query = query
	} else {
		panic(fmt.Sprintf("illegal state: cannot overwrite existing query '%s' with query '%s': %T@%p\n%s", tb.query, query, tb, tb, tb))
	}

	return tb
}

func (tb *tmplBuilderImpl) AddPlan(p Plan) Plan {
	panic("implement me")
}

func (tb *tmplBuilderImpl) Build() (Plan, error) {
	tb.ifBuilt(fmt.Sprintf("illegal state: this %T@%p has already been built\n%s", tb, tb, tb), true)
	tb.built = true
	return tb, nil
}

func (tb *tmplBuilderImpl) String() string {
	return fmt.Sprintf("(%T@%p) built: %t keys: '%s' q: '%s'\n", tb, tb, tb.built, strings.Join(tb.keys, ","), tb.query)
}

func (tb *tmplBuilderImpl) Execute(handler func(result string) error) error {
	panic("implement me")
}

func (pb *planBuilderImpl) String() string {
	return pb.string(&strings.Builder{}, "", "")
}

func (pb *planBuilderImpl) string(sb *strings.Builder, leadIndent, indent string) string {
	sb.WriteString(fmt.Sprintf("%s(%T@%p) oper: '%v' built: %t \n", leadIndent, pb, pb, pb.oper, pb.built))

	for _, v := range pb.children {
		sb.WriteString(fmt.Sprintf("%s  child: %s", indent, v))
	}

	for _, v := range pb.templates {
		sb.WriteString(fmt.Sprintf("%s  template: %s", indent+"  ", v))
	}

	return sb.String()
}

func newPlanBuilder() *planBuilderImpl {
	return &planBuilderImpl{}
}

func (pb *planBuilderImpl) addChildPlanBuilder(op QueryOp) *planBuilderImpl {
	child := &planBuilderImpl{oper: op}
	pb.children = append(pb.children, child)
	return child
}

func (pb *planBuilderImpl) addTemplateBuilder() *tmplBuilderImpl {
	tb := newTmplBuilder()
	pb.templates = append(pb.templates, &tb)
	pb.active = &tb
	return &tb
}

func (pb *planBuilderImpl) Execute(handler func(result string) error) error {
	panic("implement me")
}

// Recursively builds child plans and query templates
func (pb *planBuilderImpl) Build() (Plan, error) {
	pb.built = true

	for _, v := range pb.children {
		if v.built {
			continue
		}
		if _, err := v.Build(); err != nil {
			panic(err)
		}
	}

	for _, v := range pb.templates {
		if v.built {
			continue
		}
		if _, err := v.Build(); err != nil {
			panic(err)
		}
	}

	return pb, nil
}

func (pb *planBuilderImpl) Or() PlanBuilder {
	return pb.addChildPlanBuilder(Or)
}
