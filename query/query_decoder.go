//
// Copyright 2021 Johns Hopkins University
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package query

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
)

var (
	queryT    = token("query")
	orT       = token("or")
	orArray   = token("pseudo-orarray")
	orObj     = token("pseudo-orobj")
	andT      = token("and")
	keysT     = token("keys")
	qT        = token("q")
	nilT      = token("nil")
	typeToken = token("pseudo-passType")
)

type token string

type tokenElement struct {
	t token
	b *Builder
}

type tokenStack struct {
	elements []*tokenElement
}

type PlanDecoder interface {
	Decode(config string) map[string]Plan
}

type decoder struct{}

func NewPlanDecoder() PlanDecoder {
	return decoder{}
}

func (decoder) Decode(config string) map[string]Plan {
	plans := make(map[string]Plan)
	dec := json.NewDecoder(strings.NewReader(config))
	stack := tokenStack{}

	var passTypeBuilder PlanBuilder
	var passType string

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
					stack.push(orObj, nil)
				}
			case "[":
				switch e := stack.peek(); e.t {
				case orT:
					stack.push(orArray, nil)
				}
			case "}", "]":
				if stack.size() > 0 {
					switch t, b := stack.pop(); t {
					case orArray:
						// if we have the matching brace for closing an 'or' array, then pop the stack again, retrieving
						// the 'or' builder, and build it.  We know we won't be seeing any more query templates for this builder
						if jsonToken.(json.Delim).String() == "]" {
							_, builder := stack.pop()
							if p, e := builder.Build(); e != nil {
								panic(fmt.Sprintf("error building %T@%p: %s\n%s", p, p, e.Error(), p))
							} else {
								log.Printf("built %T@%p", p, p)
							}
						}
					case orObj:
						// if we have the matching brace for closing an 'or' object, then pop the stack again, retrieving
						// the 'or' builder, and build it.  We know we won't be seeing any more query templates for this builder
						if jsonToken.(json.Delim).String() == "}" {
							_, builder := stack.pop()
							if p, e := builder.Build(); e != nil {
								panic(fmt.Sprintf("error building %T@%p: %s\n%s", p, p, e.Error(), p))
							} else {
								log.Printf("built %T@%p", p, p)
							}
						}
					case orT:
						if e := stack.peek(); e.t == typeToken {
							if p, e := (*e.b).Build(); e != nil {
								panic(fmt.Sprintf("error building %T@%p: %s\n%s", p, p, e.Error(), p))
							} else {
								log.Printf("built %T@%p", p, p)
								_, _ = stack.pop()
							}
						}
					case queryT:
						if p, e := b.Build(); e != nil {
							panic(fmt.Sprintf("error building %T@%p: %s\n%s", p, p, e.Error(), p))
						} else {
							log.Printf("built %T@%p", p, p)
						}
						if e := stack.peek(); e.t == typeToken {
							if p, e := (*e.b).Build(); e != nil {
								panic(fmt.Sprintf("error building %T@%p: %s\n%s", p, p, e.Error(), p))
							} else {
								log.Printf("built %T@%p", p, p)
								_, _ = stack.pop()
							}
						}
					case typeToken:
						if p, e := b.Build(); e != nil {
							panic(fmt.Sprintf("error building %T@%p: %s\n%s", p, p, e.Error(), p))
						} else {
							log.Printf("built %T@%p", p, p)
						}
					}
				} else {
					if p, e := passTypeBuilder.Build(); e != nil {
						panic(fmt.Sprintf("error building %T@%p: %s\n%s", p, p, e.Error(), p))
					} else {
						log.Printf("built %T@%p", p, p)
					}
				}
			}
		case string:
			switch t := token(jsonToken.(string)); t {
			case queryT:
				var tb Builder
				if b := stack.latestBuilder(); b == nil {
					tb = passTypeBuilder.(*planBuilderImpl).addTemplateBuilder()
				} else {
					tb = b.(*planBuilderImpl).addTemplateBuilder()
				}
				stack.push(queryT, &tb)
			case orT:
				var pb PlanBuilder
				var b Builder
				// If we encounter an operation token (or, and), *and* our last token added to the stack was an 'or',
				// or 'and', then we need to build and attach a PlanBuilder for handling *this* 'or's operations to the
				// previous 'or's plan builder (in the case of a nested operation), or the root PlanBuilder.
				switch e := stack.peek(); e.t {
				case orArray, orObj:
					pseudoElement := stack.popE()
					orElement := stack.popE()
					pb = (*orElement.b).(PlanBuilder).Or()
					stack.pushE(orElement)
					stack.pushE(pseudoElement)
				default:
					pb = passTypeBuilder.Or()
				}
				b = pb
				log.Printf("created plan for token '%s': %T@%p", orT, b, &b)
				stack.push(t, &b)

			// We are inside a query template object.  The object may or may not have been created depending on the order
			// the tokens were encountered.
			case keysT, qT:
				switch stack.latestBuilder().(type) {
				case *planBuilderImpl:
					// If we are seeing 'keys' or 'q' for the first time, and the latest builder is a plan builder,
					// we need to add a template builder to the plan.
					if stack.latestBuilder().(*planBuilderImpl).active == nil {
						stack.latestBuilder().(*planBuilderImpl).addTemplateBuilder()
					}
				}
				stack.push(t, nil)
			default:
				if stack.size() > 0 {
					// TODO recurse up the stack and get the most recent template builder (?)
					log.Printf("Have a value for '%s': '%v'", stack.peek().t, jsonToken)

					// add the key or query to the TemplateBuilder
					switch e := stack.peek(); e.t {
					case keysT:
						switch stack.latestBuilder().(type) {
						case *planBuilderImpl:
							stack.latestBuilder().(*planBuilderImpl).active.AddKey(jsonToken.(string))
						case *tmplBuilderImpl:
							stack.latestBuilder().(*tmplBuilderImpl).AddKey(jsonToken.(string))
						}
					case qT:
						switch stack.latestBuilder().(type) {
						case *planBuilderImpl:
							stack.latestBuilder().(*planBuilderImpl).active.AddQuery(jsonToken.(string))
						case *tmplBuilderImpl:
							stack.latestBuilder().(*tmplBuilderImpl).AddQuery(jsonToken.(string))
						}
					default:
						panic(fmt.Sprintf("Unknown token %v in 'query' object", jsonToken))
					}
				} else {
					// have a top level key representing a PASS type
					passType = jsonToken.(string)
					passTypeBuilder = newPlanBuilder()
					log.Printf("Have a PASS type: %v", passType)
					if p, exists := plans[passType]; exists {
						panic(fmt.Sprintf("illegal state: %T@%s already exists for type '%s'", p, p, passType))
					} else {
						plans[passType] = passTypeBuilder
					}
					var ptb Builder
					ptb = passTypeBuilder.(Builder)
					stack.push(typeToken, &ptb)
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
	log.Printf("pushed '%s' '%T@%p'", e.t, e.b, e.b)
}

func (ts *tokenStack) push(t token, b *Builder) {
	ts.pushE(tokenElement{t, b})
}

func (ts *tokenStack) popE() tokenElement {
	if ts.size() == 0 {
		panic("cannot pop an empty stack")
	}
	popped := ts.elements[len(ts.elements)-1]
	ts.elements = ts.elements[0 : len(ts.elements)-1]
	log.Printf("popped '%s' '%T@%p'", popped.t, popped.b, popped.b)
	return *popped
}

func (ts *tokenStack) pop() (token, Builder) {
	e := ts.popE()
	if e.b == nil {
		return e.t, nil
	}

	return e.t, *e.b
}

func (ts *tokenStack) peek() *tokenElement {
	if ts.size() == 0 {
		return &tokenElement{nilT, nil}
	}
	return ts.elements[len(ts.elements)-1]
}

func (ts *tokenStack) latestBuilder() Builder {
	if ts.size() == 0 {
		return nil
	}

	for i := range ts.elements {
		e := ts.elements[len(ts.elements)-i-1]
		if e.b != nil {
			return *e.b
		}
	}

	return nil
}

func (ts *tokenStack) size() int {
	return len(ts.elements)
}
