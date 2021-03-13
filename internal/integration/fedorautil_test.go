// +build integration

package integration

import (
	"bytes"
	"dupe-checker/env"
	"dupe-checker/model"
	"fmt"
	"github.com/knakk/rdf"
	"github.com/stretchr/testify/assert"
	"io"
	"log"
	"net/http"
	"strings"
	"testing"
)

// Culls nil elements from the provided slice
func removeNils(trips *[]*rdf.Triple) []*rdf.Triple {
	var result []*rdf.Triple
	for i := range *trips {
		if (*trips)[i] != nil {
			result = append(result, (*trips)[i])
		}
	}

	return result
}

// Copies the content from sourceUri, transforms the triples, and PUTs a new resource with the transformed content
// at targetUri.
//
// The only valid combination to perform a copy of an RDF resource with knakk/rdf is to use N-Triples serialization in
// combination with PUT.  It is particularly difficult to transform the RDF of the resource to be copied, especially
// because knakk/rdf does not allow for null relative URIs.
func copyFedoraResource(t *testing.T, environment env.Env, sourceUri, targetUri string, transformer func(triples *[]*rdf.Triple)) string {
	var req *http.Request
	var res *http.Response
	var trips []rdf.Triple
	var err error

	req, err = http.NewRequest("GET", sourceUri, nil)
	assert.Nil(t, err)
	req.SetBasicAuth(environment.FcrepoUser, environment.FcrepoPassword)
	req.Header.Add("Accept", "application/n-triples")

	res, err = httpClient.Do(req)
	defer func() { _ = res.Body.Close() }()
	assert.Nil(t, err)

	b := bytes.Buffer{}
	_, err = io.Copy(&b, res.Body)
	assert.Nil(t, err)
	assert.True(t, res.StatusCode == 200, err)

	var filteredTrips []*rdf.Triple
	filteredResource := bytes.Buffer{}
	trips, err = rdf.NewTripleDecoder(&b, rdf.Turtle).DecodeAll()
	assert.True(t, len(trips) > 0)

	for i := range trips {
		t := trips[i]
		if strings.HasPrefix(t.Pred.String(), model.PassResourceUriPrefix) ||
			strings.HasPrefix(t.Obj.String(), model.PassResourceUriPrefix) {
			filteredTrips = append(filteredTrips, &t)
		}
	}

	// replace the subject
	subject, _ := rdf.NewIRI(targetUri)
	for i := range filteredTrips {
		filteredTrips[i] = &rdf.Triple{Subj: subject, Pred: filteredTrips[i].Pred, Obj: filteredTrips[i].Obj}
	}

	transformer(&filteredTrips)
	filteredTrips = removeNils(&filteredTrips)

	filteredResource = bytes.Buffer{}
	encoder := rdf.NewTripleEncoder(&filteredResource, rdf.NTriples)
	var toEncode []rdf.Triple
	for _, triple := range filteredTrips {
		toEncode = append(toEncode, *triple)
	}
	err = encoder.EncodeAll(toEncode)
	assert.Nil(t, err)
	_ = encoder.Close()

	log.Printf("Replaced copy:\n%s", filteredResource.String())

	req, err = http.NewRequest("PUT", targetUri, bytes.NewReader(filteredResource.Bytes()))
	assert.Nil(t, err)
	req.SetBasicAuth(environment.FcrepoUser, environment.FcrepoPassword)
	req.Header.Add("Content-Type", "application/n-triples")

	res, err = httpClient.Do(req)
	defer func() { _ = res.Body.Close() }()
	assert.Nil(t, err)
	assert.True(t, res.StatusCode == 201, fmt.Sprintf("status code: %v, error: %v", res.StatusCode, err))
	b = bytes.Buffer{}
	_, err = io.Copy(&b, res.Body)
	return string(b.Bytes())
}

// Attempts to replace the content of the Fedora resource at 'uri' with the content in 'body' described by 'mediaType'.
// Note: if a resource already exists at 'uri', this func will DELETE it, and then PUT a new resource; SPARQL update is
// not used.
func replaceFedoraResource(t *testing.T, environment env.Env, uri string, body []byte, mediaType string) (err error) {
	var (
		req *http.Request
		res *http.Response
	)

	// Delete the resource at 'uri' if it exists, and get rid of the tombstone.
	if req, err = http.NewRequest("HEAD", uri, nil); err != nil {
		return err
	}
	req.SetBasicAuth(environment.FcrepoUser, environment.FcrepoPassword)
	if res, err = httpClient.Do(req); err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode <= 300 {
		// delete the existing resource
		if req, err = http.NewRequest("DELETE", uri, nil); err != nil {
			return err
		}
		req.SetBasicAuth(environment.FcrepoUser, environment.FcrepoPassword)
		if res, err = httpClient.Do(req); err != nil {
			return err
		}
		defer res.Body.Close()
		if req, err = http.NewRequest("DELETE", fmt.Sprintf("%s/fcr:tombstone", uri), nil); err != nil {
			return err
		}
		req.SetBasicAuth(environment.FcrepoUser, environment.FcrepoPassword)
		if res, err = httpClient.Do(req); err != nil {
			return err
		}
		defer res.Body.Close()
	}

	// PUT the 'body' at 'uri'
	if req, err = http.NewRequest("PUT", uri, bytes.NewReader(body)); err != nil {
		return err
	}
	req.SetBasicAuth(environment.FcrepoUser, environment.FcrepoPassword)
	req.Header.Add("Content-Type", mediaType)
	if res, err = httpClient.Do(req); err != nil {
		return err
	}
	assert.True(t, res.StatusCode < 300)
	return err
}
