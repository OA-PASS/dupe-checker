// Responsible for creating and executing HTTP requests for LDP containers
package retriever

import (
	"dupe-checker/model"
	"github.com/knakk/rdf"
	"net/http"
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
		return model.LdpContainer{}, err
	}

	defer func() { res.Body.Close() }()

	dec := rdf.NewTripleDecoder(res.Body, rdf.NTriples)
	if triples, err = dec.DecodeAll(); err != nil {
		return model.LdpContainer{}, err
	}

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
