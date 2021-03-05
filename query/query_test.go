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
	"bytes"
	"dupe-checker/model"
	_ "embed"
	"errors"
	"fmt"
	"github.com/knakk/rdf"
	"github.com/stretchr/testify/assert"
	"github.com/yourbasic/graph"
	"github.com/yourbasic/graph/build"
	"html/template"
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

//go:embed queryconfig-nested-or.json
var queryConfigNestedOr string

//go:embed queryconfig-simple.json
var queryConfigSimple string

//go:embed queryconfig-multipletypes.json
var queryConfigMultipleTypes string

//go:embed pass-journal.n3
var passJournal string

//go:embed pass-publication.n3
var passPublication string

func Test_PlanBuilderImplChildrenReturnsTemplates(t *testing.T) {

}

// insures that the Children() method of planBuilderImpl properly recurses child plans, including templates
func Test_PlanBuilderImplChildren(t *testing.T) {
	grandTemplates := []*tmplBuilderImpl{&tmplBuilderImpl{}}
	grandchild := planBuilderImpl{templates: grandTemplates}
	grandchildren := []*planBuilderImpl{&grandchild}

	childTemplates := []*tmplBuilderImpl{&tmplBuilderImpl{}}
	child := planBuilderImpl{templates: childTemplates, children: grandchildren}
	children := []*planBuilderImpl{&child}

	parentTemplates := []*tmplBuilderImpl{&tmplBuilderImpl{}}
	parent := planBuilderImpl{templates: parentTemplates, children: children}

	// parent.Children() should return the immediate child plans and templates.
	assert.Equal(t, 2, len(parent.Children()))
	assert.Equal(t, 2, len(child.Children()))
	assert.Equal(t, 1, len(grandchild.Children()))

	// recursing parent.Children() should return
	//   - parent template
	//   - child plan
	//   - child template
	//   - grandchild plan
	//   - grandchild template
	count := 0
	// note that the recursive verifier invokes the provided function on the supplied plan, so the parent itself is
	// counted.
	recursiveVerifier("", &parent, func(planType string, p Plan) {
		count++
	})
	assert.Equal(t, 6, count, &parent)
}

func recursiveCounter(p Plan, count int) int {
	count += len(p.Children())
	for _, c := range p.Children() {
		return recursiveCounter(c, count)
	}
	return count
}

// verifies the state of the plan
type planVerifier func(planType string, p Plan)

// executes the planVerifier on the given plan and recursively on all its descendents
func recursiveVerifier(planType string, p Plan, pv planVerifier) {
	pv(planType, p)

	for _, child := range p.Children() {
		recursiveVerifier(planType, child, pv)
	}
}

func Test_DecodeSimple(t *testing.T) {
	plans := decoder{}.Decode(queryConfigSimple)

	assert.NotNil(t, plans)
	assert.True(t, len(plans) > 0)
	assert.NotNil(t, plans["http://oapass.org/ns/pass#Journal"])

	expectedTotalPlanCount := 1 // just the root plan, which should be built.
	expectedTotalTemplateCount := 1
	expectedBuiltCount := expectedTotalPlanCount + expectedTotalTemplateCount

	actualBuiltCount, actualTotalPlanCount, actualTotalTemplateCount := 0, 0, 0
	recursiveVerifier("http://oapass.org/ns/pass#Journal", plans["http://oapass.org/ns/pass#Journal"],
		func(planType string, p Plan) {
			assert.Equal(t, "http://oapass.org/ns/pass#Journal", planType)
			switch p.(type) {
			case *tmplBuilderImpl:
				tmplBuilder := p.(*tmplBuilderImpl)
				assert.True(t, tmplBuilder.built)
				assert.EqualValues(t, []string{"nlmta"}, tmplBuilder.keys)
				assert.Equal(t, "{{.Scheme}}://{{.HostAndPort}}/{{.Index}}/_search?q={{$count := dec (len .KvPairs)}}{{range $i, $e := .KvPairs}}{{$e.Key}}:\"{{$e.Value | urlqueryesc }}\"{{if lt $i $count}}+{{end}}{{end}}&default_operator=AND", tmplBuilder.query)
				actualTotalTemplateCount++
			case *planBuilderImpl:
				assert.True(t, p.(*planBuilderImpl).built)
				actualTotalPlanCount++
			}
			actualBuiltCount++
		})

	assert.Equal(t, expectedBuiltCount, actualBuiltCount)
	assert.Equal(t, actualTotalTemplateCount, expectedTotalTemplateCount)
	assert.Equal(t, actualTotalPlanCount, expectedTotalPlanCount)
}

// This JSON is pathological and not allowed
//func Test_DecodeSimpleOrObject(t *testing.T) {
//	plans := decoder{}.Decode(queryConfigSimpleOrObj)
//
//	assert.NotNil(t, plans)
//	assert.True(t, len(plans) > 0)
//
//	verifyPlans(t, plans, 1, 1)
//}

func Test_DecodeSimpleOrArray(t *testing.T) {
	plans := decoder{}.Decode(queryConfigSimpleOrArray)

	assert.NotNil(t, plans)
	assert.Equal(t, 1, len(plans))

	expectedTotalPlanCount := 4 // the root plan and the or plan, and the two queries; all should be built.
	expectedBuiltCount := expectedTotalPlanCount
	verifyPlans(t, plans, expectedBuiltCount, expectedTotalPlanCount)
}

func Test_DecodeNestedOrArray(t *testing.T) {
	plans := decoder{}.Decode(queryConfigNestedOr)

	assert.NotNil(t, plans)
	assert.Equal(t, 1, len(plans))

	expectedTotalPlanCount := 6 // the root plan, two child or plans (one as a child of the other), three templates.
	expectedBuiltCount := expectedTotalPlanCount
	verifyPlans(t, plans, expectedBuiltCount, expectedTotalPlanCount)
}

func Test_DecodeMultipleTypes(t *testing.T) {
	plans := decoder{}.Decode(queryConfigMultipleTypes)

	assert.NotNil(t, plans)
	assert.Equal(t, 2, len(plans))

	expectedTotalPlanCount := 8 // one plan for each type, two 'or' plans with a total of three
	// templates for the Publication plan type, and a single template for the User plan type.
	expectedBuiltCount := expectedTotalPlanCount
	verifyPlans(t, plans, expectedBuiltCount, expectedTotalPlanCount)
}

func verifyPlans(t *testing.T, plans map[string]Plan, expectedBuiltCount, expectedTotalCount int) {
	actualTotalCount := 0
	actualBuiltCount := 0

	for planType, plan := range plans {
		log.Printf("Plan for type %s:\n%s", planType, plan)
		recursiveVerifier(planType, plan, func(planType string, plan Plan) {
			actualTotalCount++
			switch plan.(type) {
			case *tmplBuilderImpl:
				tmplBuilder := plan.(*tmplBuilderImpl)
				assert.True(t, tmplBuilder.built)
				actualBuiltCount++
				assert.NotZero(t, tmplBuilder.query)
				assert.NotZero(t, tmplBuilder.keys)
				for _, v := range tmplBuilder.keys {
					assert.NotZero(t, v)
				}
				//actualTotalTemplateCount++
			case *planBuilderImpl:
				assert.True(t, plan.(*planBuilderImpl).built)
				actualBuiltCount++
				//actualTotalPlanCount++
			}

		})

	}
	assert.Equal(t, expectedBuiltCount, actualBuiltCount)
	assert.Equal(t, expectedTotalCount, actualTotalCount)
}

func Test_Template(t *testing.T) {
	// scheme, host and port, index, type, values
	// http, elasticsearch:9200, pass, type, doi
	// http://elasticsearch:9200/pass?q=@type:<type>+doi:<doi>&default_operator=AND
	fmtStr := "%s://%s/%s?q=@type:%s+doi:%s&default_operator=AND"
	templateStr := "{{.Scheme}}://{{.HostAndPort}}/{{.Index}}?q=@type:{{.Type}}+doi:{{.Doi}}&default_operator=AND"

	tmpl, err := template.New("test").Parse(templateStr)
	assert.Nil(t, err)
	assert.NotNil(t, tmpl)

	buf := &bytes.Buffer{}

	err = tmpl.Execute(buf, struct {
		Scheme      string
		HostAndPort string
		Index       string
		Type        string
		Doi         string
	}{"http", "elasticsearch:9200", "pass", "Submission", "10.1.2.4/567"})

	assert.Nil(t, err)
	assert.Equal(t, fmt.Sprintf(fmtStr, "http", "elasticsearch:9200", "pass", "Submission", "10.1.2.4/567"), buf.String())
}

func Test_TemplateRange(t *testing.T) {
	kvps := []KvPair{{"@type", "Submission"}, {"doi", "10.1.2.4/567"}}

	funcMap := template.FuncMap{
		// The name "inc" is what the function will be called in the template text.
		"inc": func(i int) int {
			return i + 1
		},
		"dec": func(i int) int {
			return i - 1
		},
	}

	// scheme, host and port, index, type, values
	// http, elasticsearch:9200, pass, type, doi
	// http://elasticsearch:9200/pass?q=@type:<type>+doi:<doi>&default_operator=AND
	fmtStr := "%s://%s/%s?q=@type:%s+doi:%s&default_operator=AND"
	//templateStr := "{{.Scheme}}://{{.HostAndPort}}/{{.Index}}?q=@type:{{.Type}}+doi:{{.Doi}}&default_operator=AND"

	templateStr := "{{.Scheme}}://{{.HostAndPort}}/{{.Index}}?q={{$count := dec (len .KvPairs)}}{{range $i, $e := .KvPairs}}{{$e.Key}}:{{$e.Value}}{{if lt $i $count}}+{{end}}{{end}}&default_operator=AND"

	tmpl, err := template.New("test").Funcs(funcMap).Parse(templateStr)
	assert.Nil(t, err)
	assert.NotNil(t, tmpl)

	buf := &bytes.Buffer{}

	err = tmpl.Execute(buf, struct {
		Scheme      string
		HostAndPort string
		Index       string
		KvPairs     []KvPair
	}{"http", "elasticsearch:9200", "pass", kvps})

	assert.Nil(t, err)
	assert.Equal(t, fmt.Sprintf(fmtStr, "http", "elasticsearch:9200", "pass", "Submission", "10.1.2.4/567"), buf.String())

}

func TestTemplate_BuildAndEval(t *testing.T) {

	// normally templateStr and tmplBuilderImpl would be created by parsing the query json config
	// Scheme, HostAndPort, Index must come from env or on construction
	templateStr := "{{.Scheme}}://{{.HostAndPort}}/{{.Index}}?q={{$count := dec (len .KvPairs)}}{{range $i, $e := .KvPairs}}{{$e.Key}}:{{$e.Value}}{{if lt $i $count}}+{{end}}{{end}}&default_operator=AND"
	tmplBuilder := tmplBuilderImpl{
		built: false,
		keys:  []string{"@type", "doi"},
		query: templateStr,
	}

	plan, err := tmplBuilder.Build()

	assert.Nil(t, err)
	assert.IsType(t, Template{}, plan)

	tmpl := plan.(Template)
	esQuery, err := tmpl.eval([]KvPair{{"@type", "Submission"}, {"doi", "10.1.2.4/567"}})

	assert.Nil(t, err)
	assert.Equal(t, "http://elasticsearch.local:9200/pass?q=@type:Submission+doi:10.1.2.4/567&default_operator=AND", esQuery)
}

func TestTemplate_ExtractKeys(t *testing.T) {
	container, err := model.NewContainerFromReader(strings.NewReader(passJournal), rdf.NTriples)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(container.PassProperties()))

	kvp, err := extractKeys(container, []string{"journalName", "issn"})
	assert.Nil(t, err)
	assert.Equal(t, 3, len(kvp))

	assert.Equal(t, "journalName", kvp[0].Key.String())
	assert.Equal(t, "Community dentistry and oral epidemiology", kvp[0].Value)
	assert.Equal(t, "issn", kvp[1].Key.String())
	assert.Equal(t, "Online:1600-0528", kvp[1].Value)
	assert.Equal(t, "issn", kvp[2].Key.String())
	assert.Equal(t, "Print:0301-5661", kvp[2].Value)
}

func TestTemplate_ExtractMultiValue(t *testing.T) {
	container, err := model.NewContainerFromReader(strings.NewReader(passJournal), rdf.NTriples)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(container.PassProperties()))

	kvp, err := extractKeys(container, []string{"journalName", "issn*"})
	assert.Nil(t, err)
	assert.Equal(t, 3, len(kvp))

	assert.Equal(t, "journalName", kvp[0].Key.String())
	assert.Equal(t, "Community dentistry and oral epidemiology", kvp[0].Value)
	assert.Equal(t, "issn", kvp[1].Key.String())
	assert.Equal(t, "Online:1600-0528", kvp[1].Value)
	assert.Equal(t, "issn", kvp[2].Key.String())
	assert.Equal(t, "Print:0301-5661", kvp[2].Value)
}

func TestTemplate_ExtractMissingRequiredKeys(t *testing.T) {
	container, err := model.NewContainerFromReader(strings.NewReader(passJournal), rdf.NTriples)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(container.PassProperties()))

	kvp, err := extractKeys(container, []string{"moo", "foo"})
	assert.Zero(t, kvp)
	assert.NotNil(t, err)

	assert.True(t, errors.Is(err, ErrMissingRequiredKey))
	assert.True(t, strings.Contains(err.Error(), "moo"))
	assert.True(t, strings.Contains(err.Error(), "foo"))
}

func TestTemplate_Execute(t *testing.T) {
	// Normally the container is provided by the repository visitor but here we read it off of the filesystem.
	container, err := model.NewContainerFromReader(strings.NewReader(passPublication), rdf.NTriples)
	assert.Nil(t, err)
	assert.Equal(t, 5, len(container.PassProperties()))

	// normally templateStr and tmplBuilderImpl would be created by parsing the query json config
	// Scheme, HostAndPort, Index must come from env or on construction
	// DOIs need to be quoted!!!
	templateStr := "{{.Scheme}}://{{.HostAndPort}}/{{.Index}}/_search?q={{$count := dec (len .KvPairs)}}{{range $i, $e := .KvPairs}}{{$e.Key}}:\"{{$e.Value}}\"{{if lt $i $count}}+{{end}}{{end}}&default_operator=AND"
	tmplBuilder := tmplBuilderImpl{
		built: false,
		keys:  []string{"@type", "doi"},
		query: templateStr,
	}

	plan, err := tmplBuilder.Build()
	assert.IsType(t, Template{}, plan)

	tmpl := plan.(Template)

	processedResult := false
	_, err = tmpl.Execute(container, func(result interface{}) (bool, error) {
		assert.Nil(t, err)
		assert.IsType(t, Match{}, result)

		m := result.(Match)

		assert.NotZero(t, m.QueryUrl)
		assert.Equal(t, 1, m.HitCount)
		assert.Equal(t, 1, len(m.MatchingUris))
		assert.Equal(t, container.Uri(), m.MatchingUris[0])

		processedResult = true

		return true, nil
	})

	assert.Nil(t, err)
	assert.True(t, processedResult)
}

func TestPlanAndTemplate_ExecuteSimpleOrArray(t *testing.T) {
	// Boilerplate and sanity checks to decode the query plan for a Journal
	journalType := "http://oapass.org/ns/pass#Journal"
	plans := decoder{}.Decode(queryConfigSimpleOrArray)
	assert.NotNil(t, plans)
	assert.Equal(t, 1, len(plans))
	expectedTotalPlanCount := 4 // the root plan and the or plan, and the two queries; all should be built.
	expectedBuiltCount := expectedTotalPlanCount
	verifyPlans(t, plans, expectedBuiltCount, expectedTotalPlanCount)
	assert.NotZero(t, plans[journalType])

	// Boilerplate and sanity checks for reading in a Journal container off of the filesystem
	// We're going to execute the query plan for the Journal, which will query ElasticSearch for any duplicates

	// Normally the container is provided by the repository visitor but here we read it off of the filesystem.
	container, err := model.NewContainerFromReader(strings.NewReader(passJournal), rdf.NTriples)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(container.PassProperties()))
	assert.Equal(t, "http://fcrepo:8080/fcrepo/rest/journals/00/a6/ff/f7/00a6fff7-e4a9-4764-affd-5e9e74f5947f", container.Uri())
	assert.Equal(t, "Journal", container.PassType())
	isPass, resourceType := container.IsPassResource()
	assert.True(t, isPass)
	assert.Equal(t, "http://oapass.org/ns/pass#Journal", resourceType)

	resultProcessTriggered := 0
	resultProcessSuccessfully := 0
	// Execute the parent plan
	// *note* all the ISSNs are on the query, when we only need one to match
	if _, err := plans[journalType].Execute(container, func(result interface{}) (bool, error) {
		resultProcessTriggered++

		assert.IsType(t, Match{}, result)

		m := result.(Match)

		assert.Equal(t, 1, m.HitCount, "Expecting exactly 1 result, but got %d.  Query URL was: %s", m.HitCount, m.QueryUrl)

		//// sanity check container
		//isPass, passType := container.IsPassResource()
		//assert.True(t, isPass)
		//assert.Equal(t, "http://oapass.org/ns/pass#Journal", passType)
		//assert.Equal(t, "Journal", container.PassType())
		//assert.Equal(t, "http://fcrepo:8080/fcrepo/rest/journals/00/a6/ff/f7/00a6fff7-e4a9-4764-affd-5e9e74f5947f", container.Uri())
		//assert.Equal(t, 3, len(container.PassProperties()))
		//
		//match := result.(Match)
		//
		//assert.Equal(t, "http://fcrepo:8080/fcrepo/rest/journals/00/a6/ff/f7/00a6fff7-e4a9-4764-affd-5e9e74f5947f", match.PassUri)
		//assert.Equal(t, 1, match.HitCount)
		//assert.NotZero(t, match.QueryUrl)
		//assert.Equal(t, "Journal", match.PassType)
		//assert.Equal(t, match.HitCount, len(match.MatchingUris))

		resultProcessSuccessfully++
		return false, nil // don't short circuit
	}); err != nil {
		assert.Fail(t, fmt.Sprintf("%s", err.Error()))
	}

	assert.Equal(t, 2, resultProcessTriggered)
	assert.Equal(t, 2, resultProcessSuccessfully)

}

func TestPlanAndTemplate_ExecuteSimple(t *testing.T) {
	// Boilerplate and sanity checks to decode the query plan for a Journal
	journalType := "http://oapass.org/ns/pass#Journal"
	plans := decoder{}.Decode(queryConfigSimple)
	assert.NotNil(t, plans)
	assert.Equal(t, 1, len(plans)) // should be only one plan for the Journal type
	expectedTotalPlanCount := 2    // the root plan and the single query
	expectedBuiltCount := expectedTotalPlanCount
	verifyPlans(t, plans, expectedBuiltCount, expectedTotalPlanCount)
	assert.NotZero(t, plans[journalType])

	// Boilerplate and sanity checks for reading in a Journal container off of the filesystem
	// We're going to execute the query plan for the Journal, which will query ElasticSearch for any duplicates

	// Normally the container is provided by the repository visitor but here we read it off of the filesystem.
	container, err := model.NewContainerFromReader(strings.NewReader(passJournal), rdf.NTriples)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(container.PassProperties()))
	assert.Equal(t, "http://fcrepo:8080/fcrepo/rest/journals/00/a6/ff/f7/00a6fff7-e4a9-4764-affd-5e9e74f5947f", container.Uri())
	assert.Equal(t, "Journal", container.PassType())
	isPass, resourceType := container.IsPassResource()
	assert.True(t, isPass)
	assert.Equal(t, "http://oapass.org/ns/pass#Journal", resourceType)

	resultProcessTriggered := false
	resultProcessSuccessfully := false
	// Execute the parent plan
	if _, err := plans[journalType].Execute(container, func(result interface{}) (bool, error) {
		resultProcessTriggered = true

		// sanity check container
		isPass, passType := container.IsPassResource()
		assert.True(t, isPass)
		assert.Equal(t, "http://oapass.org/ns/pass#Journal", passType)
		assert.Equal(t, "Journal", container.PassType())
		assert.Equal(t, "http://fcrepo:8080/fcrepo/rest/journals/00/a6/ff/f7/00a6fff7-e4a9-4764-affd-5e9e74f5947f", container.Uri())
		assert.Equal(t, 3, len(container.PassProperties()))

		match := result.(Match)

		assert.Equal(t, "http://fcrepo:8080/fcrepo/rest/journals/00/a6/ff/f7/00a6fff7-e4a9-4764-affd-5e9e74f5947f", match.PassUri)
		assert.Equal(t, 1, match.HitCount)
		assert.NotZero(t, match.QueryUrl)
		assert.Equal(t, "Journal", match.PassType)
		assert.Equal(t, match.HitCount, len(match.MatchingUris))

		resultProcessSuccessfully = true
		return true, nil
	}); err != nil {
		assert.Fail(t, fmt.Sprintf("%s", err.Error()))
	}

	assert.True(t, resultProcessTriggered)
	assert.True(t, resultProcessSuccessfully)

}

/*
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
					popped := queryTokenStack.popE()
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
					queryTokenStack.popE()
				}
			}
		case string:
			switch token(t.(string)) {
			case queryT:
				queryTokenStack.pushE(queryT)
			case orT:
				queryTokenStack.pushE(orT)
				// create a new TemplateBuilder, add it to the PlanBuilder, and set the state as the
				// active template being built
				tmplBuilder = builder.Or()
			case andT:
				queryTokenStack.pushE(andT)
				// create a new TemplateBuilder, add it to the PlanBuilder, and set the state as the
				// active template being built
				//tmplBuilder = builder.And()
			case keysT:
				queryTokenStack.pushE(keysT)
			case qT:
				queryTokenStack.pushE(qT)
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
