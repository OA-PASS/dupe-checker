package query

import (
	"dupe-checker/model"
	"fmt"
	"strings"
)

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
