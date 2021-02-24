package query

import (
	_ "embed"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"github.com/yourbasic/graph"
	"github.com/yourbasic/graph/build"
	"io"
	"log"
	"strings"
	"testing"
)

//go:embed queryconfig.json
var queryConfig string

//go:embed queryconfig-simple-or-array.json
var queryConfigSimpleOrArray string

//go:embed queryconfig-simple-or-obj.json
var queryConfigSimpleOrObj string

func Test_DecodeConfig2(t *testing.T) {
	plans := decoder{}.Decode(queryConfigSimpleOrObj)

	assert.NotNil(t, plans)
	assert.True(t, len(plans) > 0)

	for k, v := range plans {
		log.Printf("Plan for type %s:\n%s", k, v)
		if v, ok := v.(*planBuilderImpl); ok {
			assert.True(t, v.built)
			for sub := v.subordinate; sub != nil; {
				assert.True(t, sub.built)
				sub = sub.subordinate
			}

			assert.True(t, len(v.templateBuilders) > 0)
			for _, tmplBuilder := range v.templateBuilders {
				assert.True(t, tmplBuilder.built)
				assert.NotZero(t, tmplBuilder.query)
				assert.NotZero(t, tmplBuilder.keys)
				for _, v := range tmplBuilder.keys {
					assert.NotZero(t, v)
				}
			}
		} else {
			assert.True(t, ok, "plan not an instance of *planBuilderImpl, was %T", v)
		}
	}
}

func Test_DecodeConfig(t *testing.T) {
	plans := make(map[string]Plan)
	var passType string
	var tmplBuilder TemplateBuilder
	builder := newPlanBuilder()
	dec := json.NewDecoder(strings.NewReader(queryConfigSimpleOrObj))
	level := 0
	queryTokenStack := tokenStack{[]token{}}

	for {
		t, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		//log.Printf("handling: %v", t)

		switch t.(type) {
		case json.Delim:
			switch t.(json.Delim).String() {
			case "{":
				level++
			case "}":
				level--
				if queryTokenStack.size() > 0 {
					popped := queryTokenStack.pop()
					switch popped {
					case orT:
						if built, err := tmplBuilder.Build(); err != nil {
							log.Fatal(err)
						} else {
							builder.AddPlan(built)
						}
					case qT:
						// TODO
					case queryT:
						if p, err := builder.Build(); err != nil {
							log.Fatal(err)
						} else {
							plans[passType] = p
						}
					default:
						// TODO
						log.Fatalf("Unhandled popped token %v", popped)
					}
				}
			case "]":
				if queryTokenStack.size() > 0 {
					queryTokenStack.pop()
				}
			}
		case string:
			switch token(t.(string)) {
			case queryT:
				queryTokenStack.push(queryT)
			case orT:
				queryTokenStack.push(orT)
				// create a new TemplateBuilder, add it to the PlanBuilder, and set the state as the
				// active template being built
				tmplBuilder = builder.Or()
			case andT:
				queryTokenStack.push(andT)
				// create a new TemplateBuilder, add it to the PlanBuilder, and set the state as the
				// active template being built
				//tmplBuilder = builder.And()
			case keysT:
				queryTokenStack.push(keysT)
			case qT:
				queryTokenStack.push(qT)
			default:
				if queryTokenStack.size() > 0 {
					if tmplBuilder == nil {
						log.Fatalf("no template builder present (has PlanBuilder().Or() or PlanBuilder.And() been invoked and stored?)")
					}
					log.Printf("Have a value for '%s': %v", queryTokenStack.peek(), t)
					// add the key or query to the TemplateBuilder
					switch queryTokenStack.peek() {
					case keysT:
						tmplBuilder.AddKey(t.(string))
					case qT:
						tmplBuilder.AddQuery(t.(string))
					default:
						log.Fatalf("Unknown token %v in 'query' object", t)
					}
				} else {
					// have a top level key representing a PASS type
					log.Printf("Have a PASS type: %v", t)
					passType = t.(string)
					//_ = builder.ForResource(t.(string))
				}
			}
		}
		//log.Printf("query stack: %v", queryTokenStack)
	}

	log.Printf("%s", builder)

	/*
		for {
			t, err := dec.Token()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("%T: %v", t, t)
			if dec.More() {
				fmt.Printf(" (more)")
			}
			fmt.Printf("\n")
		}
	*/
}

func Test_Protoype(t *testing.T) {
	g := graph.New(5)
	g.Add(0, 1)
	g.Add(0, 2)
	g.Add(1, 3)
	g.Add(1, 4)

	//g.Visit(0, func(w int, c int64) bool {
	//	log.Printf("Visiting %d", w)
	//	return false
	//})

	visitAll(g)
}

func Test_BuildVirtualGraph(t *testing.T) {
	virtualG := build.Kn(5)

	visitAll(virtualG)
}

func visitAll(g graph.Iterator) {
	for v := 0; v < g.Order(); v++ {
		graph.Sort(g).Visit(v, func(w int, c int64) bool {
			log.Printf("Visiting %d", w)
			return false
		})
	}
}
