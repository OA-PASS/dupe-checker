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
	"dupe-checker/env"
	"dupe-checker/model"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"text/template"
)

var ErrMissingRequiredKey = errors.New("query: missing required key(s)")
var ErrPerformingElasticSearchRequest = errors.New("query: error performing search")

// escapes the string to be palatable for an elastic search query
var urlQueryEscFunc = func(query string) string {
	new := strings.ReplaceAll(url.PathEscape(query), ":", "%3F")
	return strings.ReplaceAll(new, "&", "%26")
}

var incFunc = func(i int) int {
	return i + 1
}

var decFunc = func(i int) int {
	return i - 1
}

// returns true if the key may have multiple values, e.g. an issn or locatorId
var isMultiFunc = func(key Key) bool {
	return key.IsMulti()
}

var templateFuncs = template.FuncMap{
	// The name "inc" is what the function will be called in the template text.
	"inc":         incFunc,
	"dec":         decFunc,
	"urlqueryesc": urlQueryEscFunc,
	"ismulti":     isMultiFunc,
}

// A Key represents a field that is being used to match objects. Examples of Keys include 'journalName' or 'nlmta' on
// Journal; or 'doi', 'pmid', or 'title' on Publication.
//
// Keys are derived from the RDF predicate of the PASS resource.  The predicate http://oapass.org/ns/pass#nlmta becomes
// the Key("nlmta"); http://oapass.org/ns/pass#title becomes Key("title"), and so on.  Some RDF predicates may have multiple
// values.  The http://oapass.org/ns/pass#issn or http://oapass.org/ns/pass#locatorIds are examples.  When a Key may
// have multiple values, the Key is suffixed by an asterisk.  So the predicate http://oapass.org/ns/pass#issn becomes
// Key("issn*"); http://oapass.org/ns/pass#locatorIds, Key("locatorIds*").
//
// Each Key will have an associated field in the index.  Many times the Key and the index field have the same name, but
// sometimes they differ.  For example, the Key("issn*") has the index field name 'issns'.
type Key string

// Whether or not the Key may have multiple values.  For example, 'issn' or 'locatorIds' may have multiple values in
// PASS model.  The Key will have an asterisk as a suffix if this is the case.
func (k Key) IsMulti() bool {
	return strings.HasSuffix(string(k), "*")
}

// Answers the string representation of the Key, which conforms to the RDF predicate it is derived from.  For example,
// this method will answer 'issn' for the Key("issn*").
func (k Key) String() string {
	s := string(k)
	if k.IsMulti() {
		return s[0 : len(s)-1]
	}
	return s
}

// Answers the corresponding field of the index that this Key may be queried by.  For example, this method will answer
// 'issns' for the Key("issn*").  The mapping of a Key to the index field is currently hard-coded within this method.
// By default this method answers Key.String().
func (k Key) IndexField() string {
	switch k.String() {
	case "issn":
		return "issns"
	default:
		return k.String()
	}
}

// Associates a named key with a value; used when evaluating the template.  The value for a Key is typically extracted
// from the RDF form of a PASS resource.
type KvPair struct {
	Key   Key
	Value string
}

// Whether or not the Key may have multiple values.  For example, Key("issn*") or Key("locatorIds*").
func (kv KvPair) IsMulti() bool {
	return kv.Key.IsMulti()
}

// Encapsulates an ES query and the Keys it requires for evaluation
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
	return tb.asTemplate()
}

func (tb *tmplBuilderImpl) asTemplate() (Template, error) {
	if !tb.built {
		return Template{}, Error{
			wrapped: ErrIllegalStateNotBuilt,
			context: fmt.Sprintf("%T@%p must be built before it can be returned as a Template", tb, tb),
		}
	}

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

func (tb *tmplBuilderImpl) Execute(container model.LdpContainer, handler func(result interface{}) (bool, error)) (bool, error) {
	panic("implement me")
}

// Parameterizes the template with supplied key-value pairs and returns the query, ready to be executed
func (qt Template) eval(kvp []KvPair) (string, error) {
	if len(kvp) == 0 {
		return "", fmt.Errorf("query: cannot evaluate template, empty key-value pairs for %v (error extracting keys from the LdpContainer?)", qt)
	}
	buf := &bytes.Buffer{}
	//FIXME
	e := env.New()
	var scheme, hostandport, index, size string

	if u, err := url.Parse(e.IndexSearchBaseUri); err != nil {
		panic(fmt.Sprintf("Cannot parse %s as a URL: %s", e.IndexSearchBaseUri, err.Error()))
	} else {
		scheme = u.Scheme
		hostandport = u.Host
		temp := e.IndexSearchBaseUri[0:strings.LastIndex(e.IndexSearchBaseUri, "/_search")]
		index = temp[strings.LastIndex(temp, "/")+1:]
		size = e.IndexSearchMaxResultSize
	}

	if err := qt.Template.Execute(buf, struct {
		Scheme      string
		HostAndPort string
		Index       string
		Size        string
		KvPairs     []KvPair
	}{scheme, hostandport, index, size, kvp}); err != nil {
		return "", err
	} else {
		return buf.String(), nil
	}
}

func extractKeys(container model.LdpContainer, keys []string) ([]KvPair, error) {
	extractedKvps := make(map[string][]KvPair)

	for propKey, propVal := range container.PassProperties() {
		for _, key := range keys {
			k := Key(key)
			if !strings.HasSuffix(propKey, k.String()) {
				continue
			} else {
				for _, value := range propVal {
					// skip empty values (e.g. many Publications have an empty 'nlmta')
					if strings.TrimSpace(value) == "" {
						continue
					}
					if pairs, exists := extractedKvps[key]; exists {
						extractedKvps[key] = append(pairs, KvPair{Key(key), value})

					} else {
						extractedKvps[key] = []KvPair{{Key(key), value}}
					}
				}
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
		return nil, Error{ErrMissingRequiredKey, strings.Join(missing, ",")}
	}

	var result []KvPair

	for _, v := range extractedKvps {
		for i := range v {
			result = append(result, v[i])
		}
	}

	return result, nil
}

// Executes the provided ES query string and returns the number of hits.
func performQuery(query string, esClient ElasticSearchClient, keys []KvPair) (Match, error) {
	var err error
	var req *http.Request
	var res *http.Response
	var env = env.New()

	if req, err = http.NewRequest("GET", query, nil); err != nil {
		return Match{}, err
	}

	if res, err = esClient.http.Do(req); err != nil {
		return Match{}, err
	}

	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		err = Error{
			wrapped: ErrPerformingElasticSearchRequest,
			context: "",
		}
	}

	resbytes := &bytes.Buffer{}
	if _, err := io.Copy(resbytes, res.Body); err != nil {
		return Match{}, Error{
			wrapped: ErrPerformingElasticSearchRequest,
			context: fmt.Sprintf("unable to read body of request '%s': %s", query, err.Error()),
		}
	}

	// if the status code wasn't a 200, return the body of the response in the returned error
	if err != nil {
		if e, ok := err.(Error); ok {
			e.context = fmt.Sprintf("'%s' returned unexpected status code '%d' (%s)\n%s", query, res.StatusCode, res.Status, resbytes.String())
			err = e
		} else {
			e.context = fmt.Sprintf("'%s' returned unexpected status code '%d' (%s)", query, res.StatusCode, res.Status)
			err = e
		}
		log.Printf("executed query %s with result %s", query, err.Error())
		return Match{}, err
	}

	hits := &struct {
		Hits struct {
			Total int
			Hits  []struct {
				Source map[string]interface{} `json:"_source"`
			}
		}
	}{}

	if err = json.Unmarshal(resbytes.Bytes(), hits); err != nil {
		return Match{}, fmt.Errorf("query: unable to unmarshal body of request '%s': %w", query, err)
	}

	var matchFields []string
	for _, kvp := range keys {
		matchFields = append(matchFields, kvp.Key.String())
	}

	m := Match{
		fedoraBaseUri: env.FcrepoBaseUri,
		indexBaseUri:  env.FcrepoIndexBaseUri,
		QueryUrl:      query,
		HitCount:      hits.Hits.Total,
		MatchFields:   matchFields,
		MatchValues:   make(map[string][]string),
	}

	log.Printf("executed query %s with result %v", query, m)

	if m.HitCount == 0 {
		return m, nil
	}

	for _, hit := range hits.Hits.Hits {
		matchedUri := hit.Source["@id"].(string)
		m.MatchingUris = append(m.MatchingUris, matchedUri)
		var matchValues []string
		for _, kvp := range keys {
			switch hit.Source[kvp.Key.IndexField()].(type) {
			case string:
				matchValues = append(matchValues, hit.Source[kvp.Key.IndexField()].(string))
			case []string:
				matchValues = append(matchValues, strings.Join(hit.Source[kvp.Key.IndexField()].([]string), ","))
			}
		}
		m.MatchValues[matchedUri] = matchValues
	}

	// m.PassUri, m.PassType and any m.ContainerProperties are provided by the caller
	return m, nil
}

// Template is also a Plan.
func (qt Template) Execute(container model.LdpContainer, handler func(result interface{}) (bool, error)) (bool, error) {
	// we've been built already
	// extract the keys from the container
	// eval(...) the query
	// perform the query

	keys, err := extractKeys(container, qt.Keys)

	// if the supplied container doesn't carry the attributes required to form a query, then we should gracefully skip
	// performing this query
	if errors.Is(err, ErrMissingRequiredKey) {
		log.Printf("Skipping query evaluation for %s, resource is missing at least one key required to formulate the query: %s", container.Uri(), err.Error())
		return false, err
	}

	if query, err := qt.eval(keys); err != nil {
		return false, err
	} else {
		// invoke query, obtain result.
		if match, err := performQuery(query, ElasticSearchClient{
			http.Client{},
		}, keys); err != nil {
			return true, err
		} else {
			//match.PassType = container.
			match.PassUri = container.Uri()
			match.PassType = container.PassType()
			match.ContainerProperties.SourceLastModified = container.LastModified()
			match.ContainerProperties.SourceLastModifiedBy = container.LastModifiedBy()
			match.ContainerProperties.SourceCreated = container.Created()
			match.ContainerProperties.SourceCreatedBy = container.CreatedBy()

			if _, handlerErr := handler(match); handlerErr != nil {
				return true, handlerErr
			}
		}
	}

	return false, nil
}

func (qt Template) Children() []Plan {
	// templates do not have children
	return nil
}

func (qt Template) String() string {
	return fmt.Sprintf("%T: Keys: %s, Template: %T@%p", qt, qt.Keys, qt.Template, &qt.Template)
}
