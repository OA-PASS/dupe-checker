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

package model

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/datainq/xml-date-time"
	"github.com/knakk/rdf"
	"io"
	"log"
	"strings"
	"time"
)

const (
	// The URI prefix shared by all PASS resources
	PassResourceUriPrefix = "http://oapass.org/ns/pass#"
	// The URI prefix shared by all Fedora resources
	FedoraResourceUriPrefix = "http://fedora.info/definitions/v4/repository#"
	// The URI of the rdf:type predicate
	RdfTypeUri = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
	// The URI of the ldp:container predicate
	LdpContainsUri = "http://www.w3.org/ns/ldp#contains"
	// The RDF type URI of PASS Publications
	PassTypePublication = "http://oapass.org/ns/pass#Publication"
	// The RDF type URI of PASS Users
	PassTypeUser = "http://oapass.org/ns/pass#User"
	// The RDF type URI of PASS Grants
	PassTypeGrant = "http://oapass.org/ns/pass#Grant"
	// The RDF type URI of PASS Funders
	PassTypeFunder = "http://oapass.org/ns/pass#Funder"
	// The RDF type URI of PASS Repository Copies
	PassTypeRepositoryCopy = "http://oapass.org/ns/pass#RepositoryCopy"
	// The RDF type URI of PASS Journals
	PassTypeJournal = "http://oapass.org/ns/pass#Journal"
	// The RDF type URI of PASS Submissions
	PassTypeSubmission = "http://oapass.org/ns/pass#Submission"
)

// Represents an LDP container, including its URI, asserted types, and containment triples.
type LdpContainer struct {
	triples []rdf.Triple
}

// Create a new LDP container
func NewContainer(triples []rdf.Triple) LdpContainer {
	return LdpContainer{triples}
}

// The URI of this container
func (ldpc LdpContainer) Uri() string {
	if len(ldpc.triples) > 0 {
		return ldpc.triples[0].Subj.String()
	}
	return ""
}

// The parent URI of this container
func (ldpc LdpContainer) Parent() string {
	results := ldpc.filterPred(func(predicate string) bool {
		return predicate == fmt.Sprintf("%s%s", FedoraResourceUriPrefix, "hasParent")
	})

	if len(results) == 1 {
		return results[0]
	} else if len(results) > 1 {
		log.Printf("model: unexpected number of parents %d for resource %s", len(results), ldpc.Uri())
	}

	return ""
}

// The asserted types of this container
func (ldpc LdpContainer) Types() []string {
	return ldpc.filterPred(func(pred string) bool {
		return pred == RdfTypeUri
	})
}

// The PASS type of this container, if any (note: does not return a URI, only the remainder after trimming the
// 'http://oapass.org/ns/pass#' prefix).  See IsPassResource if you'd like the full URI.
func (ldpc LdpContainer) PassType() string {
	passType := ""
	ldpc.filterTriple(func(triple rdf.Triple) bool {
		if triple.Pred.String() == RdfTypeUri && strings.HasPrefix(triple.Obj.String(), PassResourceUriPrefix) {
			passType = strings.TrimPrefix(triple.Obj.String(), PassResourceUriPrefix)
			return true
		}
		return false
	})

	return passType
}

// Answers whether or not this container is a PASS resource (i.e. asserts a PASS-prefixed RDF type) and if yes, provides
// its type.
func (ldpc LdpContainer) IsPassResource() (bool, string) {
	matches := ldpc.filterTriple(func(triple rdf.Triple) bool {
		return triple.Pred.String() == RdfTypeUri && strings.HasPrefix(triple.Obj.String(), PassResourceUriPrefix)
	})

	if len(matches) == 1 {
		return true, matches[0].Obj.String()
	}

	return false, ""
}

// Answers the objects of all triples matching PASS resources (i.e. the object of any predicate beginning with the PASS
// prefix)
func (ldpc LdpContainer) PassProperties() map[string][]string {
	passProperties := make(map[string][]string)
	passTriples := ldpc.filterTriple(func(triple rdf.Triple) bool {
		return strings.HasPrefix(triple.Pred.String(), PassResourceUriPrefix)
	})

	for _, t := range passTriples {
		key := t.Pred.String()
		if val, exists := passProperties[key]; exists {
			passProperties[key] = append(val, t.Obj.String())
		} else {
			passProperties[key] = []string{t.Obj.String()}
		}
	}

	return passProperties
}

// Answers whether or not this container asserts the supplied type (i.e. the object of any rdf:type predicate matches)
func (ldpc LdpContainer) AssertsType(rdfType string) bool {
	matches := ldpc.filterTriple(func(triple rdf.Triple) bool {
		return triple.Pred.String() == RdfTypeUri && triple.Obj.String() == rdfType
	})

	return len(matches) > 0
}

// Answers the URIs of all resources contained by this container (i.e. all objects of the ldp:contains predicate)
func (ldpc LdpContainer) Contains() []string {
	return ldpc.filterPred(func(pred string) bool {
		return pred == LdpContainsUri
	})
}

func (ldpc LdpContainer) LastModified() time.Time {
	// http://fedora.info/definitions/v4/repository#lastModified
	var lastModifiedDateTime string
	ldpc.filterTriple(func(t rdf.Triple) bool {
		if t.Pred.String() == fmt.Sprintf("%s%s", FedoraResourceUriPrefix, "lastModified") {
			lastModifiedDateTime = t.Obj.String()
		}
		return true
	})

	if t, err := xmldatetime.Parse(lastModifiedDateTime); err != nil {
		return time.Unix(0, 0)
	} else {
		return t
	}
}

func (ldpc LdpContainer) Created() time.Time {
	// http://fedora.info/definitions/v4/repository#created
	var createdDateTime string
	ldpc.filterTriple(func(t rdf.Triple) bool {
		if t.Pred.String() == fmt.Sprintf("%s%s", FedoraResourceUriPrefix, "created") {
			createdDateTime = t.Obj.String()
		}
		return true
	})

	if t, err := xmldatetime.Parse(createdDateTime); err != nil {
		return time.Unix(0, 0)
	} else {
		return t
	}
}

func (ldpc LdpContainer) CreatedBy() string {
	// http://fedora.info/definitions/v4/repository#createdBy
	var createdBy string
	ldpc.filterTriple(func(t rdf.Triple) bool {
		if t.Pred.String() == fmt.Sprintf("%s%s", FedoraResourceUriPrefix, "createdBy") {
			createdBy = t.Obj.String()
		}
		return true
	})

	return createdBy
}

func (ldpc LdpContainer) LastModifiedBy() string {
	// http://fedora.info/definitions/v4/repository#lastModifiedBy
	var lastModBy string
	ldpc.filterTriple(func(t rdf.Triple) bool {
		if t.Pred.String() == fmt.Sprintf("%s%s", FedoraResourceUriPrefix, "lastModifiedBy") {
			lastModBy = t.Obj.String()
		}
		return true
	})

	return lastModBy
}

func marshalPassProperties(c LdpContainer, props *bytes.Buffer) error {
	if result, err := json.Marshal(c.PassProperties()); err != nil {
		return err
	} else {
		props.Write(result)
	}

	return nil
}

// Return all triples from the container that match the triple filter
func (ldpc LdpContainer) filterTriple(tripleFilter func(triple rdf.Triple) bool) []rdf.Triple {
	triples := []rdf.Triple{}
	for _, triple := range ldpc.triples {
		if tripleFilter(triple) {
			triples = append(triples, triple)
		}
	}
	return triples
}

// Return all object URIs from the container that match the predicate filter
func (ldpc LdpContainer) filterPred(predFilter func(string) bool) []string {
	types := []string{}
	for _, triple := range ldpc.triples {

		if predFilter(triple.Pred.String()) {
			types = append(types, triple.Obj.String())
		}
	}
	return types
}

func (ldpc LdpContainer) dumpTriples(w io.Writer, format rdf.Format) {
	for _, triple := range ldpc.triples {
		line := fmt.Sprintf("%s", triple.Serialize(format))
		w.Write([]byte(line))
	}
}

// Returns an LdpContainer represented by 'embeddedResource'.  The resource is expected to be a full RDF representation
// of an LDP Container in n-triples format.
//
// Intended as a test utility.
func ReadContainer(embeddedResource string) (LdpContainer, error) {
	return NewContainerFromReader(strings.NewReader(embeddedResource), rdf.NTriples)
}

// Returns an LdpContainer represented by the contents of the io.Reader.  The resource is expected to be a full RDF
// representation of an LDP Container in the specified format.
//
// Intended as a test utility.
func NewContainerFromReader(r io.Reader, f rdf.Format) (LdpContainer, error) {
	dec := rdf.NewTripleDecoder(r, f)
	if triples, err := dec.DecodeAll(); err != nil {
		return LdpContainer{}, err
	} else {
		return NewContainer(triples), nil
	}
}
