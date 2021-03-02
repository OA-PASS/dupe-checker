package model

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/knakk/rdf"
	"io"
	"log"
	"strings"
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
// 'http://oapass.org/ns/pass#' prefix).
func (ldpc LdpContainer) PassType() string {
	passType := ""
	ldpc.filterTriple(func(triple rdf.Triple) bool {
		if triple.Pred.String() == RdfTypeUri && strings.HasPrefix(triple.Obj.String(), PassResourceUriPrefix) {
			passType = strings.TrimPrefix(PassResourceUriPrefix, triple.Obj.String())
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
