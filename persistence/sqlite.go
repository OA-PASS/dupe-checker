package persistence

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/knakk/rdf"
	"log"
	"strings"

	//"database/sql"
	//"database/sql/driver"
	"dupe-checker/model"
	//"github.com/mattn/go-sqlite3"
)

const (
	selectContainerByUri = "SELECT container FROM main.containers WHERE container=?"
	selectStateByUri     = "SELECT state FROM main.containers WHERE container=?"
	updateStateByUri     = "UPDATE main.containers SET state = ? WHERE container = ?"
	updateContainerByUri = "UPDATE main.containers SET container = ?, parent = ?, contains = ?, types = ?, state = ? WHERE container = ?"
	insertState          = "INSERT INTO main.containers (container, state) VALUES (?, ?)"
	insertContainer      = "INSERT INTO main.containers (container, parent, contains, types, state) VALUES (?, ?, ?, ?, ?)"
)

type SqliteParams struct {
	User        string
	Pass        string
	MaxIdleConn int
	MaxOpenConn int
}

type sqlLiteEventStore struct {
	ctx context.Context
	db  *sql.DB
}

func NewSqlLiteStore(dsn string, params SqliteParams, ctx context.Context) (Store, error) {

	// if the database doesn't exist, create it: responsibility of the caller, indicated by DSN
	// if the tables don't exist, create them: responsibility of the caller?

	var db *sql.DB
	var err error

	if db, err = sql.Open("sqlite3", dsn); err != nil {
		log.Fatal(err.Error())
	}

	if params.MaxIdleConn > 0 {
		db.SetMaxIdleConns(params.MaxIdleConn)
	}

	if params.MaxOpenConn > 0 {
		db.SetMaxOpenConns(params.MaxOpenConn)
	}

	if err = db.Ping(); err != nil {
		return sqlLiteEventStore{}, err
	}

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS main.containers (container text UNIQUE NOT NULL, parent text, contains text, types text, state integer NOT NULL)")

	if err != nil {
		return sqlLiteEventStore{}, err
	}

	_, err = db.Exec("CREATE INDEX IF NOT EXISTS main.parent_index ON containers (parent)")

	if err != nil {
		return sqlLiteEventStore{}, err
	}

	/*
		rows, err = db.Query("SELECT name FROM sqlite_schema")//" WHERE type = 'table'")

		if err != nil {
			log.Fatalf(err.Error())
		}

		var tableName *string
		tmp := ""
		tableName = &tmp
		for rows.Next() {
			if err = rows.Scan(tableName); err != nil {
				log.Fatalf(err.Error())
			}
			log.Printf("Got table name: %s", *tableName)
		}

		return nil, fmt.Errorf("store: Not Implemented")
	*/

	return sqlLiteEventStore{
		ctx: ctx,
		db:  db,
	}, nil
}

func (store sqlLiteEventStore) StoreUri(containerUri string, s State) error {
	var r *sql.Rows
	var err error

	defer func() {
		if _, err := store.db.Exec("ROLLBACK TRANSACTION"); err != nil && !strings.Contains(err.Error(), "no transaction is active") {
			log.Printf("%v", NewErrTx(rollback, containerUri, err, "persistence", "StoreUri"))
		}
	}()

	if _, err = store.db.Exec("BEGIN IMMEDIATE TRANSACTION"); err != nil {
		return NewErrTx(begin, containerUri, err, "persistence", "StoreUri")
	}

	if r, err = store.db.Query(selectContainerByUri, containerUri); err != nil {
		return NewErrQuery(selectContainerByUri, err, "persistence", "StoreUri", containerUri)
	}

	defer func() {
		if err := r.Close(); err != nil {
			log.Printf("%v", NewErrClose(err, "persistence", "StoreUri"))
		}
	}()

	isUpdate := r.Next() // container exists
	r.Close()

	if isUpdate {
		if _, err = store.db.Exec(updateStateByUri, s, containerUri); err != nil {
			return NewErrQuery(updateStateByUri, err, "persistence", "StoreUri", string(s), containerUri)
		}
	} else {
		if _, err = store.db.Exec(insertState, containerUri, s); err != nil {
			return NewErrQuery(insertState, err, "persistence", "StoreUri", containerUri, string(s))
		}
	}

	if _, err := store.db.Exec("COMMIT TRANSACTION"); err != nil {
		return NewErrTx(commit, containerUri, err, "persistence", "StoreUri")
	}

	return nil
}

func (store sqlLiteEventStore) StoreContainer(c model.LdpContainer, s State) error {
	var r *sql.Rows
	var err error

	defer func() {
		if _, err := store.db.Exec("ROLLBACK TRANSACTION"); err != nil && !strings.Contains(err.Error(), "no transaction is active") {
			log.Printf("%v", NewErrTx(rollback, c.Uri(), err, "persistence", "StoreContainer"))
		}
	}()

	if _, err = store.db.Exec("BEGIN IMMEDIATE TRANSACTION"); err != nil {
		return NewErrTx(begin, c.Uri(), err, "persistence", "StoreContainer")
	}

	if r, err = store.db.Query(selectContainerByUri, c.Uri()); err != nil {
		return NewErrQuery(selectContainerByUri, err, "persistence", "StoreContainer", c.Uri())
	}

	defer func() {
		if err := r.Close(); err != nil {
			log.Printf("%v", NewErrClose(err, "persistence", "StoreContainer"))
		}
	}()

	isUpdate := r.Next()
	r.Close() // container exists

	if isUpdate {
		if _, err = store.db.Exec(updateContainerByUri, c.Uri(), c.Parent(), strings.Join(c.Contains(), ","), strings.Join(c.Types(), ","), s, c.Uri()); err != nil {
			return NewErrQuery(selectContainerByUri, err, "persistence", "StoreContainer", c.Uri(), c.Parent(), strings.Join(c.Contains(), ","), strings.Join(c.Types(), ","), string(s), c.Uri())
		}
	} else {
		if _, err = store.db.Exec(insertContainer, c.Uri(), c.Parent(), strings.Join(c.Contains(), ","), strings.Join(c.Types(), ","), s); err != nil {
			return NewErrQuery(insertContainer, err, "persistence", "StoreContainer", c.Uri(), c.Parent(), strings.Join(c.Contains(), ","), strings.Join(c.Types(), ","), string(s))
		}
	}

	if _, err := store.db.Exec("COMMIT TRANSACTION"); err != nil {
		return NewErrTx(commit, c.Uri(), err, "persistence", "StoreContainer")
	}

	return nil
}

func (store sqlLiteEventStore) Retrieve(uri string) (State, error) {
	var r *sql.Rows
	var err error
	state := Unknown

	if r, err = store.db.Query(selectStateByUri, uri); err != nil {
		return state, NewErrQuery(selectStateByUri, err, "persistence", "Retrieve", uri)
	}

	defer r.Close()

	if r.Next() {
		if err = r.Scan(&state); err != nil {
			return state, NewErrRowScan(selectStateByUri, err, "persistence", "Retrieve", uri)
		}

		return state, nil
	}

	return state, NewErrNoResults(selectStateByUri, "persistence", "Retrieve", uri)
}

func (store sqlLiteEventStore) retrieveContainer(uri string) (model.LdpContainer, error) {
	var r *sql.Rows
	var err error

	if r, err = store.db.Query("SELECT container, parent, contains, types FROM main.containers WHERE container=?", uri); err != nil {
		return model.LdpContainer{}, err
	}

	defer r.Close()

	if r.Next() {
		var (
			container, parent, contains, types []byte
		)
		if err = r.Scan(&container, &parent, &contains, &types); err != nil {
			return model.LdpContainer{}, nil
		}

		triples := []rdf.Triple{}

		for _, v := range strings.Split(string(contains), ",") {
			subj, _ := rdf.NewIRI(string(container))
			pred, _ := rdf.NewIRI(model.LdpContainsUri)
			obj, _ := rdf.NewIRI(v)
			triples = append(triples, rdf.Triple{
				Subj: rdf.Subject(subj),
				Pred: rdf.Predicate(pred),
				Obj:  rdf.Object(obj),
			})
		}

		return model.NewContainer(triples), nil
	}

	return model.LdpContainer{}, fmt.Errorf("persistence: no container found: %s", uri)
}
