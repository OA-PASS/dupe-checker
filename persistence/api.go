package persistence

import (
	"dupe-checker/model"
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
