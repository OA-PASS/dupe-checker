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
	"dupe-checker/persistence"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
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
	s := string(k)
	return strings.HasSuffix(s, "*") ||
		strings.HasSuffix(s, "*=")
}

// Whether or not the value for the key requires expansion
func (k Key) RequiresExpansion() bool {
	s := string(k)
	return strings.HasSuffix(s, "=") ||
		strings.HasSuffix(s, "=*")
}

// Answers the string representation of the Key, which conforms to the RDF predicate it is derived from.  For example,
// this method will answer 'issn' for the Key("issn*"), or 'publication' for the Key("publication*=").
func (k Key) String() string {
	s := string(k)
	if (k.IsMulti() && !k.RequiresExpansion()) ||
		(!k.IsMulti() && k.RequiresExpansion()) {
		return s[0 : len(s)-1]
	} else if k.IsMulti() && k.RequiresExpansion() {
		return s[0 : len(s)-2]
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

type KeyList []KvPair

// Returns a slice of unique Keys in the list
func (kl KeyList) KeySet() KeySet {
	resultMap := make(map[Key]interface{})
	var resultSlice []Key

	for _, kvp := range kl {
		if _, exists := resultMap[kvp.Key]; !exists {
			resultMap[kvp.Key] = nil
			resultSlice = append(resultSlice, kvp.Key)
		}
	}

	return resultSlice
}

type KeySet []Key

func (ks KeySet) Contains(key Key) bool {
	for i := range ks {
		if key.String() == ks[i].String() {
			return true
		}
	}

	return false
}

// Whether or not the Key may have equivalent values that need to be checked.  For example, Key("submitter=") or
// Key("publication=").
func (kv KvPair) RequiresExpansion() bool {
	return kv.Key.RequiresExpansion()
}

// Answers a slice of equivalent KvPairs from the store; the result may be empty
func (kv KvPair) Expand(store *persistence.Store) ([]KvPair, error) {
	return expandKvp(kv, store)
}

func (kv KvPair) Equals(other KvPair) bool {
	return string(kv.Key) == string(other.Key) &&
		kv.Value == other.Value
}

func expandKvp(kv KvPair, store *persistence.Store) ([]KvPair, error) {
	var expanded []KvPair
	if res, err := (*store).ExpandValue(kv.Value); err != nil {
		return []KvPair{}, err
	} else {
		for i := range res {
			expanded = append(expanded, KvPair{kv.Key, res[i]})
		}
	}

	return expanded, nil
}

// QueryPairs is a slice of KvPairs that may be evaluated, resulting in a valid Elastic Search query
type QueryPairs []KvPair

// Replace the given KvPair with its replacement.  If the KvPair to replace doesn't exist in the QueryPairs, this
// method is a noop
func (qp QueryPairs) Replace(replace KvPair, replacement KvPair) {
	for i := range qp {
		if qp[i].Equals(replace) {
			qp[i] = replacement
		}
	}
}

func (qp QueryPairs) Clone() QueryPairs {
	var clone []KvPair

	for i := range qp {
		clone = append(clone, KvPair{qp[i].Key, qp[i].Value})
	}

	return clone
}

// ExpandedPairs is a slice of QueryPairs.  An ExpandedPairs represents the results of expansion on a []KvPair.
type ExpandedPairs struct {
	qps []QueryPairs
}

// Expand the ExpandedPairs by copying each QueryPairs and replacing every instance of 'replace' with 'replacement'
func (ep ExpandedPairs) Expand(replace, replacement KvPair) ExpandedPairs {
	for i := range ep.qps {
		clonedQp := ep.qps[i].Clone()
		clonedQp.Replace(replace, replacement)
		ep.qps = append(ep.qps, clonedQp)
	}

	return ExpandedPairs{ep.qps}
}

// Encapsulates an ES query and the Keys it requires for evaluation
type Template struct {
	Template template.Template
	Keys     []string
	store    *persistence.Store
}

type tmplBuilderImpl struct {
	built bool
	keys  []string
	query string
	store *persistence.Store
}

func newTmplBuilder(store *persistence.Store) tmplBuilderImpl {
	return tmplBuilderImpl{store: store}
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
			store:    tb.store,
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

	// Get the *unique* list of keys
	var matchFields []string
	for _, k := range KeyList(keys).KeySet() {
		matchFields = append(matchFields, k.String())
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

	// for each hit:
	//   store the matching uri in the slice Match.MatchingUris
	//   store the matching values in the map Match.MatchValues, keyed by the MatchingUri.
	for _, hit := range hits.Hits.Hits {
		matchedUri := hit.Source["@id"].(string)
		m.MatchingUris = append(m.MatchingUris, matchedUri)
		m.MatchValues[matchedUri] = extractValuesFromHitSource(KeyList(keys).KeySet(), hit.Source)
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

	// TODO move the http.Client{} used by performQuery to the template struct like store.
	client := http.Client{}

	if query, err := qt.eval(keys); err != nil {
		return false, err
	} else {
		// invoke query, obtain result.
		if match, err := performQuery(query, ElasticSearchClient{
			client,
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

func expand(queryPair QueryPairs, store *persistence.Store) (ExpandedPairs, error) {

	result := ExpandedPairs{qps: []QueryPairs{queryPair}}

	for _, kvp := range queryPair {
		if !kvp.RequiresExpansion() {
			continue
		}
		if expandedPairs, err := kvp.Expand(store); err != nil {
			return ExpandedPairs{}, err
		} else {
			for _, expandedPair := range expandedPairs {
				result = result.Expand(kvp, expandedPair)
			}
		}
	}

	return result, nil
}

func (qt Template) Children() []Plan {
	// templates do not have children
	return nil
}

func (qt Template) String() string {
	return fmt.Sprintf("%T: Keys: %s, Template: %T@%p", qt, qt.Keys, qt.Template, &qt.Template)
}

// Expects a Hit _source JSON object like the following as a map[string]interface{}.  Extracts the values for each
// key in the KeySet from the hit source map.  Each key in the KeySet will align with its values in the result slice;
// e.g. the zeroth Key in the KeySet will have its values in the zeroth element of the result slice.
//
// {
//  "firstName" : "Elizabeth",
//  "lastName" : "Daugherty Biddison",
//  "@type" : "User",
//  "displayName" : "Elizabeth Daugherty Biddison",
//  "roles" : [
//    "submitter"
//  ],
//  "locatorIds" : [
//    "johnshopkins.edu:employeeid:00016197",
//    "johnshopkins.edu:hopkinsid:79DUP9",
//    "johnshopkins.edu:jhed:edaughe2"
//  ],
//  "@id" : "https://pass.jhu.edu/fcrepo/rest/users/75/3a/e2/19/753ae219-63c2-4aef-a930-9e419734a279",
//  "@context" : "https://oa-pass.github.io/pass-data-model/src/main/resources/context-3.1.jsonld",
//  "email" : "edaughe2@johnshopkins.edu"
//}
func extractValuesFromHitSource(keys KeySet, hitSource map[string]interface{}) (result []string) {
	for _, key := range keys {
		if value, exists := hitSource[key.IndexField()]; !exists {
			continue
		} else {
			switch t := value.(type) {
			case string:
				result = append(result, value.(string))
			case map[string]interface{}:
				// TODO
			case []interface{}:
				array := value.([]interface{})
				var values []string
				var sample interface{}
				if len(array) == 0 {
					continue
				} else {
					sample = array[0]
				}
				switch arrayType := sample.(type) {
				case string:
					for i := range array {
						values = append(values, array[i].(string))
					}
					sort.Slice(values, func(i, j int) bool {
						return values[i] < values[j]
					})
					result = append(result, strings.Join(values, ","))
				default:
					panic(fmt.Sprintf("Unhandled array type processing a Hit []interface{} in _source: %v", arrayType))
				}
			default:
				panic(fmt.Sprintf("Unhandled type processing Hit _source JSON: %v", t))
			}
		}
	}

	return
}
