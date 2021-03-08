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

package persistence

import (
	"dupe-checker/model"
	"github.com/mattn/go-sqlite3"
	"math"
	"reflect"
	"time"
)

type retryStore struct {
	retryInterval   time.Duration
	backoffFactor   float64
	maxTries        int
	underlyingStore Store
	errors          []error
}

type retryCallback func() error

func NewRetrySqliteStore(store Store, retryInterval time.Duration, backoffFactor float64, maxTries int, errors ...error) Store {
	return retryStore{
		retryInterval:   retryInterval,
		backoffFactor:   backoffFactor,
		maxTries:        maxTries,
		underlyingStore: store,
		errors:          errors,
	}
}

func (rs retryStore) StoreContainer(c model.LdpContainer, s State) error {
	return rs.retry(rs.maxTries, rs.retryInterval, func() error {
		return rs.underlyingStore.StoreContainer(c, s)
	})
}

func (rs retryStore) StoreUri(containerUri string, s State) error {
	return rs.retry(rs.maxTries, rs.retryInterval, func() error {
		return rs.underlyingStore.StoreUri(containerUri, s)
	})
}

func (rs retryStore) Retrieve(uri string) (State, error) {
	return rs.underlyingStore.Retrieve(uri)
}

func (rs retryStore) StoreDupe(sourceUri, targetUri, passType string, matchedOn, matchedValues []string, attribs DupeContainerAttributes) error {
	return rs.retry(rs.maxTries, rs.retryInterval, func() error {
		return rs.underlyingStore.StoreDupe(sourceUri, targetUri, passType, matchedOn, matchedValues, attribs)
	})
}

func (rs retryStore) ExpandValue(value string) ([]string, error) {
	var expanded []string
	var err error
	rs.retry(rs.maxTries, rs.retryInterval, func() error {
		expanded, err = rs.underlyingStore.ExpandValue(value)
		return err
	})
	return expanded, err
}

func (rs retryStore) retry(triesLeft int, retryInterval time.Duration, callback retryCallback) error {
	// invoke the callback and capture the error
	var err error
	err = toError(reflect.ValueOf(callback).Call([]reflect.Value{}))

	if err == nil {
		return nil
	}

	// if the error is in the list if errors we retry on, then recurse (trying again)
	for _, targetErr := range rs.errors {
		if checkError(err, targetErr) {
			//log.Printf("Retrying callback after sleeping for %d ms (triesLeft: %d, err: %v)", retryInterval.Milliseconds(), triesLeft, err)
			// back off
			time.Sleep(retryInterval)

			triesLeft = triesLeft - 1
			if triesLeft == 0 {
				return NewErrMaxRetry(err, rs.maxTries)
			}

			retryFloat := float64(retryInterval.Nanoseconds()) * rs.backoffFactor
			retryInterval = time.Duration(int64(math.Floor(retryFloat)))

			// recurse (retry again)
			err = rs.retry(triesLeft, retryInterval, callback)

			// only retry for the first error in the list
			break
		}
		// next error
	}

	return err
}

func toError(values []reflect.Value) error {
	if len(values) == 0 {
		return nil
	}

	if len(values) > 1 {
		panic("Unexpected number of values after invoking retry callback")
	}

	v := values[0]

	if v.IsNil() {
		return nil
	}

	return v.Interface().(error)
}

func checkError(caught, target error) bool {
	//log.Printf("Checking caught error %v against target error %v", caught, target)
	var ok bool
	var se StoreErr
	var sqliteErr sqlite3.Error

	if se, ok = caught.(StoreErr); !ok {
		return false
	}

	if sqliteErr, ok = se.Underlying.(sqlite3.Error); !ok {
		return false
	}

	if _, ok = target.(sqlite3.ErrNo); !ok {
		return false
	}

	return sqliteErr.Code == target.(sqlite3.ErrNo)
}
