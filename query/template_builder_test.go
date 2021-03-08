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
	"dupe-checker/persistence"
	"github.com/stretchr/testify/assert"
	"log"
	"sort"
	"strings"
	"testing"
	"text/template"
)

func Test_UrlQueryEscFunc(t *testing.T) {
	assert.Equal(t, "%3F", urlQueryEscFunc(":"))
	assert.Equal(t, "%26", urlQueryEscFunc("&"))
	assert.Equal(t, "%26amp%3B", urlQueryEscFunc("&amp;"))
	assert.Equal(t, "%26lt%3Bi%26gt%3B", urlQueryEscFunc("&lt;i&gt;"))
	assert.Equal(t, "%20", urlQueryEscFunc(" "))
}

func Test_UrlQueryEscFuncPathologicalTitle(t *testing.T) {
	title := "Loop-Mediated Isothermal Amplification for Detection of the 5.8S Ribosomal Ribonucleic Acid Internal Transcribed Spacer 2 Gene Found in &lt;i&gt;Trypanosoma brucei gambiense&lt;/i&gt;."
	log.Printf("%s", urlQueryEscFunc(title))
}

func Test_Sort(t *testing.T) {
	locators := "johnshopkins.edu:hopkinsid:427B19,johnshopkins.edu:jhed:kgreen66,johnshopkins.edu:employeeid:00273821"
	x := strings.Split(locators, ",")
	sort.Slice(x, func(i, j int) bool {
		return x[i] < x[j]
	})

	log.Printf("%v", x)
}

func Test_Eval(t *testing.T) {
	var tmpl *template.Template
	var err error
	var query string

	templateStr := "{{.Scheme}}://{{.HostAndPort}}/{{.Index}}/_search?q={{$count := dec (len .KvPairs)}}{{range $i, $e := .KvPairs}}{{$e.Key.IndexField}}:{{if ismulti $e.Key}}*{{$e.Value | urlqueryesc}}*{{else}}\"{{$e.Value | urlqueryesc}}\"{{end}}{{if lt $i $count}}+{{end}}{{end}}&default_operator=AND"
	tmpl, err = template.New("test").Funcs(templateFuncs).Parse(templateStr)

	assert.Nil(t, err)

	template := Template{
		Template: *tmpl,
		Keys:     []string{"journalName", "issn*"},
	}

	query, err = template.eval([]KvPair{
		{"journalName", "Current opinion in endocrinology & diabetes"},
		{"issn*", "Print%3F1068-3097"}})

	assert.Nil(t, err)

	assert.False(t, strings.Contains(query, "&amp;"))
	assert.True(t, strings.Contains(query, "%20%26%20"))
}

func TestKeyList_KeySet(t *testing.T) {
	list := KeyList([]KvPair{{Key("one"), "moo"}, {Key("one"), "boo"}, {Key("two"), "foo"}})
	set := list.KeySet()
	assert.EqualValues(t, []Key{Key("one"), Key("two")}, set)
}

func Test_KeyIsMulti(t *testing.T) {
	notMultiA := "notmulti"
	notMultiB := "notmulti="
	multikeyA := "multi*"
	multikeyB := "multi=*"
	multikeyC := "multi*="

	assert.False(t, Key(notMultiA).IsMulti())
	assert.False(t, Key(notMultiB).IsMulti())
	assert.True(t, Key(multikeyA).IsMulti())
	assert.True(t, Key(multikeyB).IsMulti())
	assert.True(t, Key(multikeyC).IsMulti())
}

func Test_KeyRequiresExpansion(t *testing.T) {
	noExpansionA := "noexpansion"
	noExpansionB := "exp*"
	expansionA := "exp=*"
	expansionB := "exp*="
	expansionC := "exp="

	assert.False(t, Key(noExpansionA).RequiresExpansion())
	assert.False(t, Key(noExpansionB).RequiresExpansion())
	assert.True(t, Key(expansionA).RequiresExpansion())
	assert.True(t, Key(expansionB).RequiresExpansion())
	assert.True(t, Key(expansionC).RequiresExpansion())
}

func Test_KeyIndexField(t *testing.T) {
	assert.Equal(t, "issns", Key("issn*").IndexField())
	assert.Equal(t, "issns", Key("issn").IndexField())
	assert.Equal(t, "doi", Key("doi*").IndexField())
	assert.Equal(t, "doi", Key("doi*=").IndexField())
	assert.Equal(t, "doi", Key("doi=*").IndexField())
}

func Test_KeyString(t *testing.T) {
	assert.Equal(t, "issn", Key("issn*").String())
	assert.Equal(t, "issn", Key("issn").String())
	assert.Equal(t, "doi", Key("doi*").String())
	assert.Equal(t, "doi", Key("doi*=").String())
	assert.Equal(t, "doi", Key("doi=*").String())
}

func Test_KvPairEquals(t *testing.T) {
	assert.True(t, KvPair{"issn", "value"}.Equals(KvPair{"issn", "value"}))
	assert.False(t, KvPair{"issn*", "value"}.Equals(KvPair{"issn", "value"}))
	assert.False(t, KvPair{"issn", "valueA"}.Equals(KvPair{"issn", "valueB"}))
	assert.False(t, KvPair{"keyA", "value"}.Equals(KvPair{"keyB", "value"}))
}

func Test_KvPairExpand(t *testing.T) {
	dsn := ":memory:"
	var store persistence.Store
	var err error

	if store, err = persistence.NewSqlLiteStore(dsn, persistence.SqliteParams{MaxIdleConn: 2, MaxOpenConn: 10}, nil); err != nil {
		assert.Fail(t, err.Error())
	}

	// In the real world, we may have a KvPair for a 'submitter' (base URIs are elided in the database)
	//   e.g.: Key: 'submitter' Value: '/users/fb/43/4e/24/fb434e24-05d9-40db-b723-375427f536ff'
	//
	// If the User referenced by 'submitter' ('375427f536ff' in this example) has duplicates,
	// then the Expand() method for the KvPair will return all other KvPairs that reference the duplicate Users.
	//
	// So if the User '/users/fb/43/4e/24/fb434e24-05d9-40db-b723-375427f536ff' has three duplicate resources:
	//    - /users/d3/21/7b/3e/d3217b3e-5269-4f39-855e-63a77cad6384,
	//      /users/ac/95/78/55/ac957855-ba06-434a-bb9f-505070e46f86,
	//      /users/8b/37/da/5e/8b37da5e-971c-4fae-a5d9-2fd80ee5370d
	// Then calling expand on KvPair{'submitter', '/users/fb/43/4e/24/fb434e24-05d9-40db-b723-375427f536ff'} will
	// return three KvPairs that reference the duplicates:
	//	  - KvPair{'submitter', '/users/d3/21/7b/3e/d3217b3e-5269-4f39-855e-63a77cad6384'}
	//	  - KvPair{'submitter', '/users/ac/95/78/55/ac957855-ba06-434a-bb9f-505070e46f86'}
	//	  - KvPair{'submitter', '/users/8b/37/da/5e/8b37da5e-971c-4fae-a5d9-2fd80ee5370d'}

	value := "moo"
	kvPair := KvPair{Key("source"), value}  // this is our KvPair that we wish to expand
	duplicateMoos := []string{"mo", "moooo", "mu"}      // we need to pre-populate the db with duplicates
	var expected []KvPair                               // these are expected KvPairs that will be returned by Expand()

	// First, store some duplicate values
	for _, dupe := range duplicateMoos {
		if err := store.StoreDupe(value, dupe, "type", []string{"match"}, []string{dupe}, persistence.DupeContainerAttributes{}); err != nil {
			assert.Fail(t, err.Error())
		}
		expected = append(expected, KvPair{kvPair.Key, dupe})
	}

	// expand kvPair
	if result, err := kvPair.Expand(&store); err != nil {
		assert.Fail(t, err.Error())
	} else {
		assert.EqualValues(t, expected, result)
	}
}
func Test_QueryPairsReplace(t *testing.T) {
	kvA := KvPair{Key("A"), "Foo"}
	kvB := KvPair{Key("B"), "Moo"}
	kvC := KvPair{Key("C"), "Boo"}
	qp := QueryPairs([]KvPair{kvA, kvB})

	qp.Replace(kvB, kvC)

	assert.EqualValues(t, []KvPair{kvA, kvC}, qp)
	assert.NotEqualValues(t, []KvPair{kvA, kvB}, qp)
}

func Test_QueryPairsClone(t *testing.T) {
	kvA := KvPair{Key("A"), "Foo"}
	kvB := KvPair{Key("B"), "Moo"}
	kvC := KvPair{Key("C"), "Boo"}
	qp := QueryPairs([]KvPair{kvA, kvB, kvC})

	clone := qp.Clone()

	assert.NotSame(t, &qp, &clone)
	assert.NotSame(t, &qp[0], &clone[0])
}

func Test_QueryPairsExpand(t *testing.T) {
	kvA := KvPair{Key("A"), "Foo"}
	kvB := KvPair{Key("B"), "Moo"}
	kvC := KvPair{Key("C"), "Boo"}
	qp := QueryPairs([]KvPair{kvA, kvB})

	exp := ExpandedPairs{[]QueryPairs{qp}}
	assert.Equal(t, 1, len(exp.qps))
	assert.EqualValues(t, []KvPair{kvA, kvB}, (exp.qps)[0])

	expanded := exp.Expand(kvB, kvC)
	assert.Equal(t, 2, len(expanded.qps))
	assert.EqualValues(t, []KvPair{kvA, kvB}, (expanded.qps)[0])
	assert.EqualValues(t, []KvPair{kvA, kvC}, (expanded.qps)[1])
}

func Test_QueryPairsExpandSingleKvPair(t *testing.T) {
	kvA := KvPair{Key("A"), "Foo"}
	kvB := KvPair{Key("B"), "Moo"}
	qp := QueryPairs([]KvPair{kvA})

	exp := ExpandedPairs{[]QueryPairs{qp}}
	assert.Equal(t, 1, len(exp.qps))
	assert.EqualValues(t, []KvPair{kvA}, (exp.qps)[0])

	expanded := exp.Expand(kvA, kvB)
	assert.Equal(t, 2, len(expanded.qps))
	assert.EqualValues(t, []KvPair{kvA}, (expanded.qps)[0])
	assert.EqualValues(t, []KvPair{kvB}, (expanded.qps)[1])
}

func Test_FuncExpand(t *testing.T) {
	dsn := ":memory:"
	var store persistence.Store
	var err error
	var expandedPairs ExpandedPairs

	if store, err = persistence.NewSqlLiteStore(dsn, persistence.SqliteParams{MaxIdleConn: 2, MaxOpenConn: 10}, nil); err != nil {
		assert.Fail(t, err.Error())
	}

	value := "moo"
	kvPair := KvPair{Key("source="), value}  // this is our KvPair that we wish to expand
	duplicateMoos := []string{"mo", "moooo"}// "mu"}      // we need to pre-populate the db with duplicates
	var expected []KvPair                               // these are expected KvPairs that will be returned by Expand()

	// First, store some duplicate values
	for _, dupe := range duplicateMoos {
		if err := store.StoreDupe(value, dupe, "type", []string{"match"}, []string{dupe}, persistence.DupeContainerAttributes{}); err != nil {
			assert.Fail(t, err.Error())
		}
		expected = append(expected, KvPair{kvPair.Key, dupe})
	}

	// expand kvPair
	expandedPairs, err = expand([]KvPair{kvPair}, &store)
	assert.Nil(t, err)
	assert.Equal(t, 4, len(expandedPairs.qps))
}


