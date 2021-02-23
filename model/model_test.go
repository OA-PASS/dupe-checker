package model

import (
	"bytes"
	_ "embed"
	"github.com/piprate/json-gold/ld"
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
)

//go:embed pass-funder.n3
var n3Funder string

//go:embed pass-user.n3
var n3User string

//go:embed pass-usercontainer.n3
var n3UserContainer string

func Test_LdpContainer_SingleFunderResource(t *testing.T) {
	c, _ := ReadContainer(n3Funder)

	//c.dumpTriples(os.Stderr, rdf.NTriples)

	isPass, passResourceType := c.IsPassResource()
	name, hasNameProperty := c.PassProperties()["http://oapass.org/ns/pass#name"]

	assert.True(t, isPass)
	assert.Equal(t, "http://oapass.org/ns/pass#Funder", passResourceType)
	assert.Equal(t, "http://fcrepo:8080/fcrepo/rest/funders", c.Parent())
	assert.Equal(t, "http://fcrepo:8080/fcrepo/rest/funders/34/53/1a/2f/34531a2f-e014-4f30-a7a2-d3476868f32c", c.Uri())
	assert.True(t, hasNameProperty)
	assert.Equal(t, "EXCELA HEALTH", name[0])
}

func Test_LdpContainer_SingleUserResource(t *testing.T) {
	c, _ := ReadContainer(n3User)

	//c.dumpTriples(os.Stderr, rdf.NTriples)
	locators, hasLocatorIds := c.PassProperties()["http://oapass.org/ns/pass#locatorIds"]

	assert.True(t, hasLocatorIds)
	assert.Equal(t, 2, len(locators))
}

func Test_LdpContainer_UsersContainerResource(t *testing.T) {
	c, _ := ReadContainer(n3UserContainer)
	assert.Equal(t, 0, len(c.PassProperties()))
}

func Test_LdpContainer_JsonLd(t *testing.T) {
	proc := ld.NewJsonLdProcessor()
	opts := ld.NewJsonLdOptions("")
	opts.Format = "application/n-quads"

	doc, err := proc.FromRDF(n3User, opts)

	assert.Nil(t, err)
	assert.NotNil(t, doc)

	ld.PrintDocument("Result:", doc)
}

func Test_MarshalPassProperties(t *testing.T) {
	c, _ := ReadContainer(n3User)
	buf := bytes.Buffer{}
	err := marshalPassProperties(c, &buf)
	assert.Nil(t, err)

	log.Printf("%s", buf.String())
}
