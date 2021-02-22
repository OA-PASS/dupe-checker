package persistence

import (
	"dupe-checker/model"
	"errors"
	"fmt"
	"github.com/knakk/rdf"
	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
	"time"
)

const (
	storeContainer = iota
	storeUri
	retrieve
)

type call int

type probe struct {
	invoked  call
	withArgs []string
	retVal   []interface{}
}

func (p *probe) StoreContainer(c model.LdpContainer, s State) error {
	p.invoked = storeContainer
	p.withArgs = []string{c.Uri(), fmt.Sprintf("%d", s)}
	if p.retVal[0] == nil {
		return nil
	}

	if err, ok := p.retVal[1].(error); ok {
		return err
	}

	panic("No return value satisfies signature (error)")
}

func (p *probe) StoreUri(containerUri string, s State) error {
	p.invoked = storeUri
	p.withArgs = []string{containerUri, fmt.Sprintf("%d", s)}
	if p.retVal[0] == nil {
		return nil
	}

	if err, ok := p.retVal[1].(error); ok {
		return err
	}

	panic("No return value satisfies signature (error)")
}

func (p *probe) Retrieve(uri string) (State, error) {
	p.invoked = retrieve
	p.withArgs = []string{uri}

	if p.retVal[1] == nil {
		return p.retVal[0].(State), nil
	}

	if err, ok := p.retVal[1].(error); ok {
		return p.retVal[0].(State), err
	}

	panic("No return value satisfies signature (State, error)")
}

func TestRetryStore_InvokesUnderlyingRetrieve(t *testing.T) {
	probe := &probe{
		retVal: []interface{}{Started, (error)(nil)},
	}

	underTest := retryStore{
		time.Duration(100),
		1.2,
		3,
		probe,
		[]error{sqlite3.ErrNo(8)},
	}

	uri := "http://www.google.com"
	state, err := underTest.Retrieve(uri)

	assert.Equal(t, retrieve, int(probe.invoked))
	assert.Equal(t, uri, probe.withArgs[0])
	assert.Equal(t, state, probe.retVal[0])
	assert.Nil(t, err)
}

func TestRetryStore_InvokesUnderlyingStoreUri(t *testing.T) {
	probe := &probe{
		retVal: []interface{}{(error)(nil)},
	}

	underTest := retryStore{
		time.Duration(100),
		1.2,
		3,
		probe,
		[]error{sqlite3.ErrNo(8)},
	}

	uri := "http://www.google.com"
	err := underTest.StoreUri(uri, Started)

	assert.Equal(t, storeUri, int(probe.invoked))
	assert.Equal(t, uri, probe.withArgs[0])
	assert.Nil(t, err)
}

func TestRetryStore_InvokesUnderlyingStoreContainer(t *testing.T) {
	probe := &probe{
		retVal: []interface{}{(error)(nil)},
	}

	underTest := retryStore{
		time.Duration(100),
		1.2,
		3,
		probe,
		[]error{sqlite3.ErrNo(8)},
	}

	uri := "http://www.google.com"
	iri, _ := rdf.NewIRI(uri)
	err := underTest.StoreContainer(model.NewContainer([]rdf.Triple{{iri, iri, iri}}), Started)

	assert.Equal(t, storeContainer, int(probe.invoked))
	assert.Equal(t, uri, probe.withArgs[0])
	assert.Nil(t, err)
}

func TestRetryStore_MaxRetry(t *testing.T) {
	underTest := retryStore{
		1 * time.Second,
		1.2,
		3,
		nil,
		[]error{sqlite3.ErrNo(8)},
	}

	start := time.Now()
	result := underTest.retry(underTest.maxTries, underTest.retryInterval, func() error {
		return sqlite3.Error{
			Code: sqlite3.ErrNo(8),
		}
	})

	assert.True(t, time.Now().Sub(start).Seconds() >
		underTest.retryInterval.Seconds()*underTest.backoffFactor*underTest.backoffFactor*underTest.backoffFactor)
	assert.NotNil(t, result)
	assert.True(t, errors.Is(result, ErrMaxRetry))
}

func TestRetryStore_RetrySuccessTry2(t *testing.T) {
	underTest := retryStore{
		1 * time.Second,
		1.2,
		3,
		nil,
		[]error{sqlite3.ErrNo(8)},
	}

	try := 0

	result := underTest.retry(underTest.maxTries, underTest.retryInterval, func() error {
		try++

		if try == 2 { // succeed
			return nil
		}

		return sqlite3.Error{
			Code: sqlite3.ErrNo(8),
		}
	})

	assert.Nil(t, result)
}

func Test_ToErrorZeroValues(t *testing.T) {
	assert.Nil(t, toError([]reflect.Value{}))
}

func Test_ToErrorMultipleValues(t *testing.T) {
	recoverCalled := false
	defer func() {
		recover()
		recoverCalled = true
	}()
	_ = toError([]reflect.Value{reflect.ValueOf(errors.New("moo")), reflect.ValueOf(errors.New("moo"))})
	assert.True(t, recoverCalled)
}

func Test_ToError(t *testing.T) {
	err := errors.New("moo")
	result := toError([]reflect.Value{reflect.ValueOf(err)})

	assert.EqualError(t, result, err.Error())
}

func Test_ToErrorNonErrorValue(t *testing.T) {
	recoverCalled := false
	defer func() {
		recover()
		recoverCalled = true
	}()
	randomVal := new(interface{})
	_ = toError([]reflect.Value{reflect.ValueOf(randomVal)})
	assert.True(t, recoverCalled)
}

func Test_CheckError(t *testing.T) {
	caught := sqlite3.Error{
		Code:         sqlite3.ErrNo(8),
		ExtendedCode: 0,
		SystemErrno:  0,
	}

	target := sqlite3.ErrNo(8)

	assert.True(t, checkError(caught, target))
}

func Test_CheckErrorNoMatch(t *testing.T) {
	caught := sqlite3.Error{
		Code:         sqlite3.ErrNo(8),
		ExtendedCode: 0,
		SystemErrno:  0,
	}

	target := sqlite3.ErrNo(7)

	assert.False(t, checkError(caught, target))
}

func Test_CheckErrorCaughtNotSqlite(t *testing.T) {
	caught := errors.New("not an sqlite error")
	target := sqlite3.ErrNo(8)

	assert.False(t, checkError(caught, target))
}

func Test_CheckErrorTargetNotSqlite(t *testing.T) {
	caught := sqlite3.Error{
		Code:         sqlite3.ErrNo(8),
		ExtendedCode: 0,
		SystemErrno:  0,
	}
	target := errors.New("not an sqlite error")

	assert.False(t, checkError(caught, target))
}
