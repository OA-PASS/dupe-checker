// Responsible for creating and executing HTTP requests for LDP containers
package retrieve

import (
	"dupe-checker/model"
	"fmt"
	"github.com/knakk/rdf"
	"io"
	"log"
	"net/http"
	"os"
)

const (
	// HTTP media type used to request an N-triples representation of a resource
	NTriplesMediaType = "application/n-triples"
)

// Retrieve the resource identified by the URI.  The resource is expected to be an LDP container.
type Retriever interface {
	Get(uri string) (model.LdpContainer, error)
}

type retriever struct {
	httpClient *http.Client
	username   string
	password   string
	useragent  string
}

// Retrieve the resource identified by the URI.  The resource is expected to be an LDP container.
func (r retriever) Get(uri string) (model.LdpContainer, error) {
	var req *http.Request
	var res *http.Response
	var triples []rdf.Triple
	var err error

	if req, err = http.NewRequest("GET", uri, nil); err != nil {
		return model.LdpContainer{}, err
	} else {
		if len(r.username) > 0 {
			req.SetBasicAuth(r.username, r.password)
		}
		if len(r.useragent) > 0 {
			req.Header.Add("User-Agent", r.useragent)
		}
		req.Header.Add("Accept", NTriplesMediaType)
	}

	if res, err = r.httpClient.Do(req); err != nil {
		return model.LdpContainer{}, fmt.Errorf("retriever: error executing GET %s: %w", uri, err)
	}

	defer func() { res.Body.Close() }()

	var tmp *os.File
	if tmp, err = os.CreateTemp("", "container-*.n3"); err != nil {
		log.Fatalf("Error creating or truncating file: %v", err)
	} else {
		if _, err := io.Copy(tmp, res.Body); err != nil {
			log.Fatalf("Unable to write %s: %v", tmp.Name(), err)
		}
		if err := tmp.Close(); err != nil {
			log.Printf("Error closing %s: %v", tmp.Name(), err)
		}
	}

	if tmp, err = os.Open(tmp.Name()); err != nil {
		log.Fatalf("Unable to open file %s for reading %v", tmp.Name(), err)
	}
	dec := rdf.NewTripleDecoder(tmp, rdf.NTriples)
	if triples, err = dec.DecodeAll(); err != nil {
		//return model.LdpContainer{}, fmt.Errorf("retriever: error decoding triples of <%s>: %w", uri, err)
		log.Fatalf("retriever: error decoding triples of <%s>: %w", uri, err)
	} else {
		tmp.Close()
		os.Remove(tmp.Name())
	}

	//log.Printf("Decoded %d triples", len(triples))
	return model.NewContainer(triples), nil
}

// Creates a new Retriever instance with the supplied client.  The remaining parameters may be empty strings.
// If supplied, the username and password will be added to each request in an Authorization header.  If the useragent
// string is supplied, each request will use that value in the User-Agent header.
func New(httpClient *http.Client, username, password, useragent string) Retriever {
	r := retriever{
		httpClient: httpClient,
		username:   username,
		password:   password,
		useragent:  useragent,
	}

	return r
}