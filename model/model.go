package model

import (
	"github.com/knakk/rdf"
	"strings"
)

const (
	PassResourceUriPrefix = "http://oapass.org/ns/pass#"
	RdfTypeUri            = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
	LdpContainsUri        = "http://www.w3.org/ns/ldp#contains"
)

type LdpContainer struct {
	triples []rdf.Triple
}

func NewContainer(triples []rdf.Triple) LdpContainer {
	return LdpContainer{triples}
}

func (ldpc LdpContainer) Uri() string {
	if len(ldpc.triples) > 0 {
		return ldpc.triples[0].Subj.String()
	}
	return ""
}

func (ldpc LdpContainer) Types() []string {
	return ldpc.filterPred(func(pred string) bool {
		return pred == RdfTypeUri
	})
}

func (ldpc LdpContainer) IsPassResource() (bool, string) {
	matches := ldpc.filterTriple(func(triple rdf.Triple) bool {
		return triple.Pred.String() == RdfTypeUri && strings.HasPrefix(triple.Obj.String(), PassResourceUriPrefix)
	})

	if len(matches) == 1 {
		return true, matches[0].Obj.String()
	}

	return false, ""
}

func (ldpc LdpContainer) AssertsType(rdfType string) bool {
	matches := ldpc.filterTriple(func(triple rdf.Triple) bool {
		return triple.Pred.String() == RdfTypeUri && triple.Obj.String() == rdfType
	})

	return len(matches) > 0
}

func (ldpc LdpContainer) Contains() []string {
	return ldpc.filterPred(func(pred string) bool {
		return pred == LdpContainsUri
	})
}

func (ldpc LdpContainer) filterTriple(tripleFilter func(triple rdf.Triple) bool) []rdf.Triple {
	triples := []rdf.Triple{}
	for _, triple := range ldpc.triples {
		if tripleFilter(triple) {
			triples = append(triples, triple)
		}
	}
	return triples
}

func (ldpc LdpContainer) filterPred(predFilter func(string) bool) []string {
	types := []string{}
	for _, triple := range ldpc.triples {

		if predFilter(triple.Pred.String()) {
			types = append(types, triple.Obj.String())
		}
	}

	return types
}
