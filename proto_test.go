package main

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/knakk/rdf"
	"github.com/stretchr/testify/assert"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

const NTriples = "application/n-triples"

type LdpContainer struct {
	triples []rdf.Triple
}

func (ldpc LdpContainer) Types() []string {
	//types := []string{}
	//for _, triple := range ldpc.triples {
	//	if triple.Pred.String() == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" {
	//		types = append(types, triple.Obj.String())
	//	}
	//}
	//
	//return types
	return ldpc.filterPred(func(pred string) bool {
		return pred == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
	})
}

func (ldpc LdpContainer) IsPassResource() (bool, string) {
	matches := ldpc.filterTriple(func(triple rdf.Triple) bool {
		return triple.Pred.String() == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" && strings.HasPrefix(triple.Obj.String(), "http://oapass.org/ns/pass#")
	})

	if len(matches) == 1 {
		return true, matches[0].Obj.String()
	}

	return false, ""
}

func (ldpc LdpContainer) AssertsType(rdfType string) bool {
	matches := ldpc.filterTriple(func(triple rdf.Triple) bool {
		return triple.Pred.String() == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" && triple.Obj.String() == rdfType
	})

	return len(matches) > 0
}

func (ldpc LdpContainer) Contains() []string {
	return ldpc.filterPred(func(pred string) bool {
		return pred == "http://www.w3.org/ns/ldp#contains"
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

func Test(t *testing.T) {

	allTrips := decodeTriples(t, "/tmp/user-container.n3")

	for _, trip := range allTrips {
		if strings.HasSuffix(trip.Pred.String(), "#contains") {
			continue
		}
		log.Printf("Triple: %v", trip)
	}

	ldpc := LdpContainer{allTrips}
	for _, containerType := range ldpc.Types() {
		log.Printf("Container type: %s", containerType)
	}

	log.Printf("Contains %d users", len(ldpc.Contains()))
	isPass, _ := ldpc.IsPassResource()
	assert.False(t, isPass)
}

func decodeTriples(t *testing.T, file string) []rdf.Triple {
	userF, err := os.Open(file)
	assert.Nil(t, err)
	rdfDec := rdf.NewTripleDecoder(userF, rdf.NTriples)

	trip, err := rdfDec.DecodeAll()
	assert.Nil(t, err)
	assert.NotNil(t, trip)
	return trip
}

func Test_Fcrepo_Semaphore(t *testing.T) {
	//artificialLimit := -1
	maxFcrepoRequests := 100
	container := LdpContainer{triples: decodeTriples(t, "/tmp/submission-container.n3")}
	fcrepoThrottle := make(chan int, maxFcrepoRequests)
	fcrepoReqs := make(chan string)
	fcrepoClient := &http.Client{
		Timeout: 120 * time.Second,
	}

	counter := struct {
		mu sync.Mutex
		count int
	}{}

	isPass, _ := container.IsPassResource()
	assert.False(t, isPass)

	go func() {
		for uri := range fcrepoReqs {
			fcrepoThrottle <- 1
			log.Printf("Reading from channel: %s", uri)
			go func(uri string) {
				req, err := http.NewRequest("GET", uri, nil)
				assert.Nil(t, err)

				counter.mu.Lock()
				counter.count++
				log.Printf("Retrieving (%d) %s", counter.count, uri)
				req.Header.Add("User-Agent", fmt.Sprintf("Semaphore Test Request %d", counter.count))
				counter.mu.Unlock()

				req.SetBasicAuth("fedoraAdmin", "moo")
				req.Header.Add("accept", NTriples)
				resp, err := fcrepoClient.Do(req)
				assert.Nil(t, err)
				assert.NotNil(t, resp)
				assert.Equal(t, 200, resp.StatusCode)
				_ = resp.Body.Close()
				_ = <- fcrepoThrottle
			}(uri)
		}
	}()

	for i, uri := range container.Contains() {
		//if i > artificialLimit && i != -1 {
		//	break
		//}
		log.Printf("Writing %d uri to channel: %s", i, uri)
		fcrepoReqs <- uri
	}
	log.Printf("Waiting for requests to finish ...")
	close(fcrepoReqs)
}

func Test_ReqStream(t *testing.T) {
	c := &http.Client{
		Timeout: 120*time.Second,
	}

	//buf := &bytes.Buffer{}
	//dec := rdf.NewTripleDecoder(buf, rdf.NTriples)

	req, _ := http.NewRequest("GET", "http://fcrepo:8080/fcrepo/rest/submissions", nil)

	req.Header.Add("Accept", NTriples)
	req.SetBasicAuth("fedoraAdmin", "moo")
	log.Printf("Executing request %s", req.RequestURI)

	var start, doReturn, firstRead, end time.Time

	start = time.Now()
	if res, err := c.Do(req); err != nil {
		log.Printf("Error executing request: %v", err)
	} else {
		doReturn = time.Now()
		log.Printf("Returing from Do(...) %s", req.RequestURI)

		assert.NotNil(t, res)
		assert.Nil(t, err)
		log.Printf("Reading body ... %s", req.RequestURI)
		var readErr error
		var line []byte
		//var prefix bool
		readErr = nil
		responseReader := bufio.NewReader(res.Body)

		for  {
			line, readErr = responseReader.ReadBytes('\n')
			if firstRead.IsZero() {
				firstRead = time.Now()
			}
			//log.Printf("Received line: %v", line)
			log.Printf("Received error: %v", readErr)
			if readErr != nil {
				break
			}
			dec := rdf.NewTripleDecoder(bytes.NewBuffer(line), rdf.NTriples)
			triple, decErr := dec.Decode()
			log.Printf("Triple: %v", triple)
			log.Printf("Decoder Err: %v", decErr)
		}

		end = time.Now()

		log.Printf("Time start: %s\nReturn from Do: %d\nFirst line read (%d): %d\nEnd (%d): %d", start.String(), doReturn.Sub(start).Milliseconds(), firstRead.Sub(start).Milliseconds(), firstRead.Sub(doReturn).Milliseconds(), end.Sub(start).Milliseconds(), end.Sub(firstRead).Milliseconds())
	}
}

func Test_ReqLdp(t *testing.T) {
	c := &http.Client{
		Timeout: 120*time.Second,
	}

	req, _ := http.NewRequest("GET", "http://fcrepo:8080/fcrepo/rest/submissions", nil)
	req.Header.Add("Accept", "application/n-triples")
	req.SetBasicAuth("fedoraAdmin", "moo")
	log.Printf("Executing request %s", req.RequestURI)

	var start, doReturn, end time.Time

	start = time.Now()
	if res, err := c.Do(req); err != nil {
		log.Printf("Error executing request: %v", err)
	} else {
		doReturn = time.Now()
		log.Printf("Returning from Do(...) %s", req.RequestURI)
		dec := rdf.NewTripleDecoder(res.Body, rdf.NTriples)

		if all, err := dec.DecodeAll(); err != nil {
			_ = LdpContainer{triples: all}

		} else {
			log.Printf("Error decoding triples: %v", err)
		}
		end = time.Now()

		log.Printf("Time start: %s\nReturn from Do: %d\nEnd (%d): %d", start.String(), doReturn.Sub(start).Milliseconds(), end.Sub(start).Milliseconds(), end.Sub(doReturn).Milliseconds())
	}
}
