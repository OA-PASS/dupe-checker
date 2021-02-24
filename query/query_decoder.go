package query

import (
	"encoding/json"
	"io"
	"log"
	"strings"
)

var (
	queryT = token("query")
	orT    = token("or")
	andT   = token("and")
	keysT  = token("keys")
	qT     = token("q")
)

type token string

type tokenStack struct {
	stack []token
}

type ConfigDecoder interface {
	Decode(config string) map[string]Plan
}

type decoder struct{}

func (decoder) Decode(config string) map[string]Plan {
	plans := make(map[string]Plan)
	dec := json.NewDecoder(strings.NewReader(config))
	level := 0
	stack := tokenStack{[]token{}}
	builder := newPlanBuilder()

	var (
		passType    string
		tmplBuilder TemplateBuilder
	)

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
				if stack.size() > 0 {
					popped := stack.pop()
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
						log.Fatalf("unhandled popped token %v", popped)
					}
				}
			case "]":
				if stack.size() > 0 {
					stack.pop()
				}
			}
		case string:
			switch token(t.(string)) {
			case queryT:
				stack.push(queryT)
			case orT:
				stack.push(orT)
				// create a new TemplateBuilder, add it to the PlanBuilder, and set the state as the
				// active template being built
				tmplBuilder = builder.Or()
			case andT:
				stack.push(andT)
				// create a new TemplateBuilder, add it to the PlanBuilder, and set the state as the
				// active template being built
				//tmplBuilder = builder.And()
			case keysT:
				stack.push(keysT)
			case qT:
				stack.push(qT)
			default:
				if stack.size() > 0 {
					if tmplBuilder == nil {
						log.Fatalf("no template builder present (has PlanBuilder().Or() or PlanBuilder.And() been invoked and stored?)")
					}
					log.Printf("Have a value for '%s': %v", stack.peek(), t)
					// add the key or query to the TemplateBuilder
					switch stack.peek() {
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
		//log.Printf("query stack: %v", stack)
	}

	//log.Printf("%s", builder)
	return plans
}

func (ts *tokenStack) push(t token) {
	ts.stack = append(ts.stack, t)
}

func (ts *tokenStack) pop() token {
	popped := ts.stack[len(ts.stack)-1]
	ts.stack = ts.stack[0 : len(ts.stack)-1]
	return popped
}

func (ts *tokenStack) peek() token {
	return ts.stack[len(ts.stack)-1]
}

func (ts *tokenStack) size() int {
	return len(ts.stack)
}
