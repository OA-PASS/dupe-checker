package main

import (
	"github.com/knakk/rdf"
	"os"
)

func main() {
	userF, _ := os.Open("/tmp/user-container.n3")
	rdfDec := rdf.NewTripleDecoder(userF, rdf.NTriples)

	rdfDec.Decode()

}
