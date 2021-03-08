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
	"github.com/stretchr/testify/assert"
	"log"
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
