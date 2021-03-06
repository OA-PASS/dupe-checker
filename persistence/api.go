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
	"errors"
	"fmt"
	"strings"
	"time"
)

//type TreeStore interface {
//	Add(leaf Leaf, terminal bool) error // adding to a terminal Leaf would be an error
//	Leaves() ([]Leaf, error)
//	TerminalLeaves() ([]Leaf, error)
//}
//
//type Leaf interface {
//	IsTerminal() bool // adding to a terminal Leaf would be an error
//	ParentUri() string
//	Uri() string
//}

const (
	commit = iota
	rollback
	begin
)

type txOp int

type StoreErr struct {
	Uri        string
	Message    string
	Wrapped    error
	Underlying error
}

func (se StoreErr) Error() string {
	return se.Message
}

func (se StoreErr) Unwrap() error {
	return se.Wrapped
}

var ErrPrepareQuery = errors.New("error preparing query")

func NewErrPrepareQuery(underlying error, query, pkg, method string) StoreErr {
	return StoreErr{
		Message:    fmt.Sprintf("%s %s: error preparing query '%s': %v", pkg, method, query, underlying),
		Wrapped:    ErrPrepareQuery,
		Underlying: underlying,
	}
}

var ErrDeserializeContainer = errors.New("error deserializing LDPC")

func NewErrDeserializeContainer(underlying error, uri, pkg, method string) StoreErr {
	return StoreErr{
		Uri:        uri,
		Message:    fmt.Sprintf("%s %s: error deserializing LDP container %s: %v", pkg, method, uri, underlying),
		Wrapped:    ErrDeserializeContainer,
		Underlying: underlying,
	}
}

var ErrSerializeContainer = errors.New("error serializing LDPC")

func NewErrSerializeContainer(underlying error, uri, pkg, method string) StoreErr {
	return StoreErr{
		Uri:        uri,
		Message:    fmt.Sprintf("%s %s: error serializing LDP container %s: %v", pkg, method, uri, underlying),
		Wrapped:    ErrSerializeContainer,
		Underlying: underlying,
	}
}

var ErrClose = errors.New("error closing result or connection")

func NewErrClose(underlying error, pkg string, method string) StoreErr {
	return StoreErr{
		Message:    fmt.Sprintf("%s %s: error closing result or connection, %v", pkg, method, underlying),
		Wrapped:    ErrClose,
		Underlying: underlying,
	}
}

var ErrQuery = errors.New("error performing query")

func NewErrQuery(query string, underlying error, pkg string, method string, placeholders ...string) StoreErr {
	return StoreErr{
		Message:    fmt.Sprintf("%s %s: error performing query '%s' (%s), %v", pkg, method, query, strings.Join(placeholders, ","), underlying),
		Wrapped:    ErrQuery,
		Underlying: underlying,
	}
}

var ErrRowScan = errors.New("rowscan error")

func NewErrRowScan(query string, underlying error, pkg string, method string, placeholders ...string) StoreErr {
	return StoreErr{
		Message:    fmt.Sprintf("%s %s: error scanning rows for query '%s' (%s), %v", pkg, method, query, strings.Join(placeholders, ","), underlying),
		Wrapped:    ErrRowScan,
		Underlying: underlying,
	}
}

var ErrNoResults = errors.New("no results for query")

func NewErrNoResults(query string, pkg string, method string, placeholders ...string) StoreErr {
	return StoreErr{
		Message: fmt.Sprintf("%s %s: no results for query '%s' (%s)", pkg, method, query, strings.Join(placeholders, ",")),
		Wrapped: ErrNoResults,
	}
}

var ErrTx = errors.New("error executing transaction")

func NewErrTx(op txOp, uri string, underlying error, pkg string, method string) StoreErr {
	var msg string
	switch op {
	case begin:
		msg = "error beginning transaction"
	case commit:
		msg = "error committing transaction"
	case rollback:
		msg = "error rolling back transaction"
	default:
		panic("unknown txOp")
	}

	return StoreErr{
		Uri:        uri,
		Message:    fmt.Sprintf("%s %s %s <%s>, %v", pkg, method, msg, uri, underlying),
		Wrapped:    ErrTx,
		Underlying: underlying,
	}
}

var ErrMaxRetry = errors.New("maximum retries for query reached")

func NewErrMaxRetry(underlying error, tries int) StoreErr {
	return StoreErr{
		Message:    fmt.Sprintf("maximum number of retries attempted (%d attempts): %v", tries, underlying),
		Wrapped:    ErrMaxRetry,
		Underlying: underlying,
	}
}

var ErrConstraint = errors.New("constraint violation")

func NewErrConstraint(query string, underlying error, pkg string, method string, placeholders ...string) StoreErr {
	return StoreErr{
		Message:    fmt.Sprintf("%s %s: constraint violation when performing query '%s' (%s), %v", pkg, method, query, strings.Join(placeholders, ","), underlying),
		Wrapped:    ErrConstraint,
		Underlying: underlying,
	}
}

type State int

const (
	Unknown State = iota
	Started
	Completed
	Processed
)

type DupeContainerAttributes struct {
	SourceCreatedBy,
	TargetCreatedBy,
	SourceLastModifiedBy,
	TargetLastModifiedBy string
	SourceCreated,
	TargetCreated,
	SourceLastModified,
	TargetLastModified time.Time
}

type Store interface {
	StoreContainer(c model.LdpContainer, s State) error
	StoreUri(containerUri string, s State) error
	Retrieve(uri string) (State, error)
	StoreDupe(source, target, passType string, matchedOn []string, attribs DupeContainerAttributes) error
}
