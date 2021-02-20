package persistence

import (
	"dupe-checker/model"
	"errors"
	"fmt"
	"strings"
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
	Message string
	Wrapped error
}

func (se StoreErr) Error() string {
	return se.Message
}

func (se StoreErr) Unwrap() error {
	return se.Wrapped
}

var ErrClose = errors.New("error closing result or connection")

func NewErrClose(underlying error, pkg string, method string) StoreErr {
	return StoreErr{
		Message: fmt.Sprintf("%s %s: error closing result or connection, %v", pkg, method, underlying),
		Wrapped: ErrClose,
	}
}

var ErrQuery = errors.New("error performing query")

func NewErrQuery(query string, underlying error, pkg string, method string, placeholders ...string) StoreErr {
	return StoreErr{
		Message: fmt.Sprintf("%s %s: error performing query '%s' (%s), %v", pkg, method, query, strings.Join(placeholders, ","), underlying),
		Wrapped: ErrQuery,
	}
}

var ErrRowScan = errors.New("rowscan error")

func NewErrRowScan(query string, underlying error, pkg string, method string, placeholders ...string) StoreErr {
	return StoreErr{
		Message: fmt.Sprintf("%s %s: error scanning rows for query '%s' (%s), %v", pkg, method, query, strings.Join(placeholders, ","), underlying),
		Wrapped: ErrRowScan,
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
		Message: fmt.Sprintf("%s %s %s <%s>, %v", pkg, method, msg, uri, underlying),
		Wrapped: ErrTx,
	}
}

type State int

const (
	Unknown State = iota
	Started
	Completed
	Processed
)

type Store interface {
	StoreContainer(c model.LdpContainer, s State) error
	StoreUri(containerUri string, s State) error
	Retrieve(uri string) (State, error)
}
