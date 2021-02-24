package query

import (
	_ "embed"
	"encoding/json"
	"github.com/yourbasic/graph"
	"github.com/yourbasic/graph/build"
	"io"
	"log"
	"strings"
	"testing"
)

//go:embed queryconfig.json
var queryConfig string

type queryTemplateObj struct {
	Keys  []string `json:"keys"`
	Query string   `json:"q"`
}

type queryObj struct {
	Op        string
	Templates []queryTemplateObj
}

type token string

var (
	query = token("query")
	or    = token("or")
	and   = token("and")
	keys  = token("keys")
	q     = token("q")
)

type tokenStack struct {
	stack []token
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

func Test_DecodeConfig(t *testing.T) {
	dec := json.NewDecoder(strings.NewReader(queryConfig))
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
					queryTokenStack.pop()
				}
			case "]":
				if queryTokenStack.size() > 0 {
					queryTokenStack.pop()
				}
			}
		case string:
			switch token(t.(string)) {
			case query:
				queryTokenStack.push(query)
			case or:
				queryTokenStack.push(or)
			case and:
				queryTokenStack.push(and)
			case keys:
				queryTokenStack.push(keys)
			case q:
				queryTokenStack.push(q)
			default:
				if queryTokenStack.size() > 0 {
					log.Printf("Have a value for '%s': %v", queryTokenStack.peek(), t)
				} else {
					// have a top level key representing a PASS type
					log.Printf("Have a PASS type: %v", t)
				}
			}
		}
		//log.Printf("query stack: %v", queryTokenStack)
	}

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
