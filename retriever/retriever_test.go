// +build integration

package retriever

import (
	"github.com/stretchr/testify/assert"
	"net/http"
	"testing"
	"time"
)

func Test_SimpleGet(t *testing.T) {
	httpClient := &http.Client{
		Timeout: 120 * time.Second,
	}

	underTest := retriever{
		httpClient: httpClient,
		username:   "fedoraAdmin",
		password:   "moo",
		useragent:  "retriever.Test_SimpleGet",
	}

	c, err := underTest.Get("http://fcrepo:8080/fcrepo/rest/submissions")

	assert.Nil(t, err)
	assert.True(t, len(c.Contains()) > 1000)
}

