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
var ErrIllegalStateNoQueryTemplatesOrChildren = errors.New("query: plan has no query templates or child plans")
var ErrIllegalStateNoQueryTemplate = errors.New("query: plan must have exactly one query template")
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

func (pb *planBuilderImpl) Execute(container model.LdpContainer, handler func(result interface{}) (bool, error)) (bool, error) {
	if !pb.built {
		// we return true here to indicate that we should short-circuit
		return true, Error{
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
			// we return true here to indicate that we should short-circuit
			return true, Error{
				wrapped: ErrIllegalStateTooManyQueryTemplates,
				context: fmt.Sprintf("%T@%p has %d query templates, but a Noop boolean operator.  Either remove the extra query templates or wrap them in a boolean operator.", pb, pb, len(pb.templates)),
			}
		}

		// returning 'true' with errors because we should short-circuit; there's been an unrecoverable error.
		if len(pb.templates) == 0 && len(pb.children) == 0 {
			// we return true here to indicate that we should short-circuit
			return true, Error{
				wrapped: ErrIllegalStateNoQueryTemplatesOrChildren,
				context: fmt.Sprintf("%T@%p has %d query templates and children; exactly one template or exactly one child is required", pb, pb, len(pb.templates)),
			}
		}

		if len(pb.templates) == 1 {
			return executeTemplate(pb.templates[0], handler, container)
		} else if len(pb.children) == 1 {
			return executeInternal(pb.children[0], container, handler)
		} else {
			panic("moo!")
		}

	case Or:
		// boolean or the results from each child; we can short circuit on the first true value
		var result bool
		var lastErr error
		for _, childPlan := range pb.children {
			shortCircuit, err := executeInternal(childPlan, container, handler)
			lastErr = err
			result = result || shortCircuit
			if result {
				return result, err
			}
		}

		// if this plan has any templates (e.g. we have a plan that nests an or within an or, and the parent or has
		// templates, they should be processed.

		for _, template := range pb.templates {
			shortCircuit, err := executeTemplate(template, handler, container)
			result = result || shortCircuit
			if result {
				return result, err
			}
		}

		return result, lastErr
	default:
		panic(fmt.Sprintf("%T@%p: operator %v unsupported", pb, pb, pb.oper))
	}
}

func executeTemplate(tmplBuilder *tmplBuilderImpl, handler func(result interface{}) (bool, error), container model.LdpContainer) (bool, error) {
	if !tmplBuilder.built {
		return true, Error{
			wrapped: ErrIllegalStateNotBuilt,
			context: fmt.Sprintf("%T@%p has not been built, and cannot be executed", tmplBuilder, tmplBuilder),
		}
	}

	if template, err := tmplBuilder.asTemplate(); err != nil {
		// if there's an error casting the template builder to a Template, we should return true, and short-circuit
		return true, err
	} else {
		return template.Execute(container, handler)
	}
}

func executeInternal(pb *planBuilderImpl, container model.LdpContainer, handler func(result interface{}) (bool, error)) (bool, error) {
	var result bool
	var lastErr error

	for _, childPlan := range pb.children {
		result, lastErr = executeInternal(childPlan, container, handler)
	}

	if len(pb.templates) == 0 {
		// TODO should be an error to get to the bottom of a tree and encounter no templates
		return result, lastErr
	}

	switch pb.oper {
	case Or:

		// we execute each template until we have executed them all or until one returns true
		for _, t := range pb.templates {
			if shortCircuit, err := executeTemplate(t, handler, container); shortCircuit {
				return true, err
			} else {
				result = result || shortCircuit
				lastErr = err
			}

			if result {
				return result, lastErr
			}
		}
	case Noop:
		// we have a stand-alone query with no boolean operator

		// insure there is exactly one query template.  We can't do anything with multiple query templates unless there is
		// a valid boolean operator, and we can't do anything with zero query templates.

		if len(pb.templates) > 1 {
			// returning true b/c we should short-circuit with this error
			return true, Error{
				wrapped: ErrIllegalStateTooManyQueryTemplates,
				context: fmt.Sprintf("%T@%p has %d query templates, but a Noop boolean operator.  Either remove the extra query templates or wrap them in a boolean operator.", pb, pb, len(pb.templates)),
			}
		}

		// returning 'true' with errors because we should short-circuit; there's been an unrecoverable error.
		if len(pb.templates) == 0 {
			return true, Error{
				wrapped: ErrIllegalStateNoQueryTemplate,
				context: fmt.Sprintf("%T@%p has %d query templates; exactly one template is required", pb, pb, len(pb.templates)),
			}
		}

		return executeTemplate(pb.templates[0], handler, container)
	default:
		panic(fmt.Sprintf("Unsupported operation: %v", pb.oper))
	}

	// TODO not sure if this is correct or not
	return false, nil
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
