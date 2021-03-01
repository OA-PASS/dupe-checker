package query

import (
	"dupe-checker/model"
	"errors"
	"fmt"
	"strings"
)

type tmplBuilderImpl struct {
	built bool
	keys  []string
	query string
}

func newTmplBuilder() tmplBuilderImpl {
	return tmplBuilderImpl{}
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

func (tb *tmplBuilderImpl) Execute(container model.LdpContainer, handler func(result string) error) error {
	panic("implement me")
}
