package model

import (
	"fmt"
	"github.com/knakk/rdf"
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
