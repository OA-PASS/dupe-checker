package query

import (
	"bytes"
	"dupe-checker/model"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"text/template"
)

var templateFuncs = template.FuncMap{
	// The name "inc" is what the function will be called in the template text.
	"inc": func(i int) int {
		return i + 1
	},
	"dec": func(i int) int {
		return i - 1
	},
}

// Associates a named key with a value; used when evaluating the template
// can this just be a map?
type KvPair struct {
	Key, Value string
}

// Encapsulates an ES query and the keys it requires for evaluation
type Template struct {
	Template template.Template
	Keys     []string
}

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

	// return a Template
	if tmpl, err := template.
		New(fmt.Sprintf("Template for %T@%p", tb, tb)).
		Funcs(templateFuncs).
		Parse(tb.query); err != nil {
		return Template{}, err
	} else {
		return Template{
			Template: *tmpl,
			Keys:     tb.keys,
		}, nil
	}
}

func (tb *tmplBuilderImpl) String() string {
	return fmt.Sprintf("(%T@%p) built: %t keys: '%s' q: '%s'\n", tb, tb, tb.built,
		strings.Join(tb.keys, ","), tb.query)
}

func (tb *tmplBuilderImpl) Execute(container model.LdpContainer, handler func(result interface{}) error) error {
	panic("implement me")
}

// Parameterizes the template with supplied key-value pairs and returns the query, ready to be executed
func (qt Template) eval(kvp []KvPair) (string, error) {
	if len(kvp) == 0 {
		return "", fmt.Errorf("query: cannot evaluate template, empty key-value pairs for %v (error extracting keys from the LdpContainer?)", qt)
	}
	buf := &bytes.Buffer{}

	if err := qt.Template.Execute(buf, struct {
		Scheme      string
		HostAndPort string
		Index       string
		KvPairs     []KvPair
	}{"http", "elasticsearch.local:9200", "pass", kvp}); err != nil {
		return "", err
	} else {
		return buf.String(), nil
	}
}

var ErrMissingKey = errors.New("missing required key(s) for query")

func extractKeys(container model.LdpContainer, keys []string) []KvPair {
	extractedKvps := make(map[string][]KvPair)

	for propKey, propVal := range container.PassProperties() {
		for _, key := range keys {
			if !strings.HasSuffix(propKey, key) {
				continue
			} else {
				for _, value := range propVal {
					if pairs, ok := extractedKvps[key]; ok {
						pairs = append(pairs, KvPair{key, value})
					} else {
						extractedKvps[key] = []KvPair{{key, value}}
					}
				}
				break
			}
		}
	}

	// non-PASS properties like RDF type are handled specially, unfortunately
	for i := range keys {
		if keys[i] == "@type" {
			// if the @type is requested, find the pass type and include it in the returned KVPairs.
			for j := range container.Types() {
				if strings.HasPrefix(container.Types()[j], model.PassResourceUriPrefix) {
					extractedKvps["@type"] = []KvPair{{"@type", strings.TrimPrefix(container.Types()[j], model.PassResourceUriPrefix)}}
				}
			}
		}
	}

	var missing []string

	for _, key := range keys {
		if _, present := extractedKvps[key]; !present {
			missing = append(missing, key)
		}
	}

	if len(missing) > 0 {
		panic(fmt.Errorf("missing required key(s) for query: %s", strings.Join(missing, ",")))
	}

	var result []KvPair

	for _, v := range extractedKvps {
		for i := range v {
			result = append(result, v[i])
		}
	}

	return result
}

// Executes the provided ES query string and returns the number of hits.
func performQuery(query string, esClient ElasticSearchClient) (Match, error) {
	var err error
	var req *http.Request
	var res *http.Response

	if req, err = http.NewRequest("GET", query, nil); err != nil {
		return Match{}, err
	}

	if res, err = esClient.http.Do(req); err != nil {
		return Match{}, err
	}

	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return Match{}, fmt.Errorf("query: request '%s' returned unexpected status code '%d' (%s)", query, res.StatusCode, res.Status)
	}

	resbytes := &bytes.Buffer{}
	if _, err = io.Copy(resbytes, res.Body); err != nil {
		return Match{}, fmt.Errorf("query: unable to read body of request '%s': %w", query, err)
	}

	hits := &struct {
		Hits struct {
			Total int
			Hits  []struct {
				Source struct {
					Id string `json:"@id"`
				} `json:"_source"`
			}
		}
	}{}

	if err = json.Unmarshal(resbytes.Bytes(), hits); err != nil {
		return Match{}, fmt.Errorf("query: unable to unmarshal body of request '%s': %w", query, err)
	}

	m := Match{
		QueryUrl: query,
		HitCount: hits.Hits.Total,
	}

	if m.HitCount == 0 {
		return m, nil
	}

	for _, hit := range hits.Hits.Hits {
		m.MatchingUris = append(m.MatchingUris, hit.Source.Id)
	}

	// m.PassUri and m.PassType are provided by the caller
	return m, nil
}

// Template is also a Plan.
func (qt Template) Execute(container model.LdpContainer, handler func(result interface{}) error) error {
	// we've been built already
	// extract the keys from the container
	// eval(...) the query
	// perform the query

	if query, err := qt.eval(extractKeys(container, qt.Keys)); err != nil {
		return err
	} else {
		// invoke query, obtain result.
		if match, err := performQuery(query, ElasticSearchClient{
			http.Client{},
		}); err != nil {
			return err
		} else if handlerErr := handler(match); handlerErr != nil {
			return handlerErr
		}
	}

	return nil
}

func (qt Template) Children() []Plan {
	// templates do not have children
	return nil
}

func (qt Template) String() string {
	return fmt.Sprintf("%T: Keys: %s, Template: %T@%p", qt, qt.Keys, qt.Template, &qt.Template)
}
