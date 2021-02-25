package query

import (
	"encoding/json"
	"fmt"
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
	nilT   = token("nil")
)

type token string

type tokenElement struct {
	t  token
	tb *TemplateBuilder
}

type tokenStack struct {
	elements []*tokenElement
}

type ConfigDecoder interface {
	Decode(config string) map[string]Plan
}

type decoder struct{}

func (decoder) Decode(config string) map[string]Plan {
	plans := make(map[string]Plan)
	dec := json.NewDecoder(strings.NewReader(config))
	stack := tokenStack{}
	builder := newPlanBuilder()

	var (
		passType    string
		tmplBuilder TemplateBuilder
	)

	for {

		jsonToken, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}

		log.Printf("handling: %v", jsonToken)

		switch jsonToken.(type) {
		case json.Delim:
			switch jsonToken.(json.Delim).String() {
			case "{":
				switch e := stack.peek(); e.t {
				case orT:
					// Verify stack element does not carry a builder at this point
					if e.tb != nil {
						panic(fmt.Sprintf("illegal state: token %s already has a %T: %p", orT, e.tb, e.tb))
					}
					// create a new TemplateBuilder, add it to the PlanBuilder, and set the state as the
					// active template being built
					tmplBuilder = builder.Or()
					log.Printf("created builder for token '%s': %T@%p", orT, tmplBuilder, tmplBuilder)
					// Update the stack element
					e.tb = &tmplBuilder
				}
			case "}":
				if stack.size() > 0 {
					switch popped := stack.popE(); popped.t {
					case orT:
						//if built, err := tmplBuilder.Build(); err != nil {
						//	log.Fatal(err)
						//} else {
						//	builder.AddPlan(built)
						//}
					case qT:
						// TODO
					case queryT:
						if p, err := builder.Build(); err != nil {
							panic(err)
						} else {
							plans[passType] = p
						}
					default:
						// TODO
						panic(fmt.Sprintf("unhandled popped token %v", popped))
					}
				}

				// We are ending the first object in this array of "or" objects
				// (we just popped the first 'q' token)
				//   "or": [
				//        {
				//          "keys": [
				//            "nlmta"
				//          ],
				//          "q": "es query for nlmta"
				//        },
				//        {
				//          "keys": [
				//            "journalName",
				//            "issn"
				//          ],
				//          "q": "es query for journalName and issn"
				//        }
				//      ]
				if stack.peek().t == orT || stack.peek().t == andT {
					log.Printf("building builder for token '%s': %T@%p", stack.peek().t, tmplBuilder, tmplBuilder)
					if built, err := tmplBuilder.Build(); err != nil {
						panic(err)
					} else {
						builder.AddPlan(built)
					}
				}
			case "]":
				_ = stack.popE()
				//switch popped {
				//case orT:
				//	if built, err := tmplBuilder.Build(); err != nil {
				//		panic(err)
				//	} else {
				//		builder.AddPlan(built)
				//	}
				//}
			case "[":
				switch e := stack.peek(); e.t {
				case orT:
					// Verify stack element does not carry a builder at this point
					if e.tb != nil {
						panic(fmt.Sprintf("illegal state: token %s already has a %T: %p", orT, e.tb, e.tb))
					}

					// create a new TemplateBuilder, add it to the PlanBuilder, and set the state as the
					// active template being built
					tmplBuilder = builder.Or()
					log.Printf("created builder for token '%s': %T@%p", orT, tmplBuilder, tmplBuilder)
					// Update the stack element
					e.tb = &tmplBuilder
				}
			}
		case string:
			switch t := token(jsonToken.(string)); t {
			case queryT, orT, andT, keysT, qT:
				stack.push(t, nil)
			default:
				if stack.size() > 0 {
					if tmplBuilder == nil {
						panic("no template builder present (has PlanBuilder().Or() or PlanBuilder.And() been invoked and stored?)")
					}
					log.Printf("Have a value for '%s': %v", stack.peek().t, jsonToken)
					// add the key or query to the TemplateBuilder
					switch e := stack.peek(); e.t {
					case keysT:
						tmplBuilder.AddKey(jsonToken.(string))
					case qT:
						tmplBuilder.AddQuery(jsonToken.(string))
					default:
						panic(fmt.Sprintf("Unknown token %v in 'query' object", jsonToken))
					}
				} else {
					// have a top level key representing a PASS type
					log.Printf("Have a PASS type: %v", jsonToken)
					passType = jsonToken.(string)
					//_ = builder.ForResource(t.(string))
				}
			}
		}
		//log.Printf("query stack: %v", stack)
	}

	//log.Printf("%s", builder)
	return plans
}

func (ts *tokenStack) pushE(element tokenElement) {
	e := element // copy the value
	ts.elements = append(ts.elements, &e)
	log.Printf("pushed %s %T@%p", e.t, e.tb, e.tb)
}

func (ts *tokenStack) push(t token, b *TemplateBuilder) {
	ts.pushE(tokenElement{t, b})
}

func (ts *tokenStack) popE() *tokenElement {
	if ts.size() == 0 {
		panic("cannot pop an empty stack")
	}
	popped := ts.elements[len(ts.elements)-1]
	ts.elements = ts.elements[0 : len(ts.elements)-1]
	return popped
}

func (ts *tokenStack) pop() (token, TemplateBuilder) {
	e := ts.popE()
	log.Printf("popped %s %T@%p", e.t, e.tb, e.tb)
	return e.t, *e.tb
}

func (ts *tokenStack) peek() tokenElement {
	if ts.size() == 0 {
		return tokenElement{nilT, nil}
	}
	return *ts.elements[len(ts.elements)-1]
}

func (ts *tokenStack) size() int {
	return len(ts.elements)
}
