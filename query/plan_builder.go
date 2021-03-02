package query

import (
	"dupe-checker/model"
	"errors"
	"fmt"
	"strings"
)

var ErrIllegalStateAlreadyBuilt = errors.New("query: object has already been built")
var ErrIllegalStateNotBuilt = errors.New("query: object has not been built")
var ErrIllegalStateTooManyQueryTemplates = errors.New("query: plan has too many query templates")
var ErrIllegalStateZeroQueryTemplates = errors.New("query: plan has zero query templates")
var ErrTypeCast = errors.New("query: error performing cast")

type planBuilderImpl struct {
	built     bool
	oper      QueryOp
	children  []*planBuilderImpl
	templates []*tmplBuilderImpl
	active    *tmplBuilderImpl
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

func (pb *planBuilderImpl) String() string {
	return pb.string(&strings.Builder{}, "", "")
}

func (pb *planBuilderImpl) string(sb *strings.Builder, leadIndent, indent string) string {
	sb.WriteString(fmt.Sprintf("%s(%T@%p) oper: '%v' built: %t \n", leadIndent, pb, pb, pb.oper, pb.built))

	for _, v := range pb.templates {
		sb.WriteString(fmt.Sprintf("%s  template: %s", leadIndent+indent, v))
	}

	for _, v := range pb.children {
		childStr := v.string(&strings.Builder{}, leadIndent, indent+"  ")
		sb.WriteString(fmt.Sprintf("%s  child: %s", leadIndent+indent, childStr))
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

func (pb *planBuilderImpl) Execute(container model.LdpContainer, handler func(result interface{}) error) error {
	if !pb.built {
		return Error{
			wrapped: ErrIllegalStateNotBuilt,
			context: fmt.Sprintf("cannot execute plan for container %s, plan %T@%p is not built",
				container.Uri(), pb, pb),
		}
	}

	switch pb.oper {
	case Noop:
		// we have a stand-alone query with no boolean operator

		// insure there is exactly one query template.  We can't do anything with multiple query templates unless there is
		// a valid boolean operator, and we can't do anything with zero query templates.

		if len(pb.templates) > 1 {
			return Error{
				wrapped: ErrIllegalStateTooManyQueryTemplates,
				context: fmt.Sprintf("%T@%p has %d query templates, but a Noop boolean operator.  Either remove the extra query templates or wrap them in a boolean operator.", pb, pb, len(pb.templates)),
			}
		}

		if len(pb.templates) == 0 {
			return Error{
				wrapped: ErrIllegalStateZeroQueryTemplates,
				context: fmt.Sprintf("%T@%p has %d query templates; exactly one template is required", pb, pb, len(pb.templates)),
			}
		}

		tmplBuilder := pb.templates[0]

		if !tmplBuilder.built {
			return Error{
				wrapped: ErrIllegalStateNotBuilt,
				context: fmt.Sprintf("%T@%p has not been built, and cannot be executed", tmplBuilder, tmplBuilder),
			}
		}

		var (
			template Template
			err      error
		)

		if template, err = tmplBuilder.asTemplate(); err != nil {
			return err
		}

		//return template.Execute(container, func(result interface{}) error {
		//	if match, ok := result.(Match); !ok {
		//		return Error{
		//			ErrTypeCast,
		//			fmt.Sprintf("expected result to be a Match, but was %T", result),
		//		}
		//	} else {
		//		// ought to make some kind of assertion of our expectations
		//		if match.HitCount != 1 {
		//			log.Printf("unexpected number of matches for %s: %d (query url was %s)", match.PassUri, match.HitCount, match.QueryUrl)
		//		}
		//	}
		//	// where we need to return true
		//	return nil
		//})
		return template.Execute(container, handler)

	//case Or:
	//	// boolean or the results from each child; we can short circuit on the first true value
	//	for _, childPlan := range pb.children {
	//		childPlan.
	//	}
	default:
		panic(fmt.Sprintf("%T@%p: operator %v unsupported", pb, pb, pb.oper))
	}
}

func executeInternal(pb *planBuilderImpl, container model.LdpContainer, handler func(result interface{}) error) (bool, error) {
	for _, childPlan := range pb.children {
		return executeInternal(childPlan, container, handler)
	}

	// FIXME
	return false, Error{}
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
