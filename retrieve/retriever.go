//
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

// Responsible for creating and executing HTTP requests for LDP containers
package retrieve

import (
	"dupe-checker/model"
	"errors"
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

	//log.Printf("retrieving %s", uri)
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

	if res.StatusCode != 200 {
		buf, _ := os.ReadFile(tmp.Name())
		return model.LdpContainer{}, errors.New(fmt.Sprintf("Error retrieving %s (status code %d): %s", uri, res.StatusCode, string(buf)))
	}

	dec := rdf.NewTripleDecoder(tmp, rdf.NTriples)
	if triples, err = dec.DecodeAll(); err != nil {
		//return model.LdpContainer{}, fmt.Errorf("retriever: error decoding triples of <%s>: %w", uri, err)
		panic(fmt.Sprintf("retriever: error decoding triples of <%s>: %s", uri, err))
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
