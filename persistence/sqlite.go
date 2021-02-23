package persistence

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
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
	createContainersTable = "CREATE TABLE IF NOT EXISTS main.containers (container text UNIQUE NOT NULL, parent text, pass text, types text, state integer NOT NULL)"
	createParentIdx       = "CREATE INDEX IF NOT EXISTS main.parent_index ON containers (parent)"
	selectContainerByUri  = "SELECT container FROM main.containers WHERE container=?"
	selectStateByUri      = "SELECT state FROM main.containers WHERE container=?"
	selectLdpcByUri       = "SELECT container, parent, pass, types FROM main.containers WHERE container=?"
	updateStateByUri      = "UPDATE main.containers SET state = ? WHERE container = ?"
	updateContainerByUri  = "UPDATE main.containers SET container = ?, parent = ?, pass = ?, types = ?, state = ? WHERE container = ?"
	insertState           = "INSERT INTO main.containers (container, state) VALUES (?, ?)"
	insertContainer       = "INSERT INTO main.containers (container, parent, pass, types, state) VALUES (?, ?, ?, ?, ?)"
	// TODO? selectChildren        = "SELECT container FROM main.containers WHERE parent=?"
)

var (
	hasParentIri, _ = rdf.NewIRI(fmt.Sprintf("%s%s", model.FedoraResourceUriPrefix, "hasParent"))
	rdfTypeIri, _   = rdf.NewIRI(model.RdfTypeUri)
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

	_, err = db.Exec(createContainersTable)

	if err != nil {
		return sqlLiteEventStore{}, err
	}

	_, err = db.Exec(createParentIdx)

	if err != nil {
		return sqlLiteEventStore{}, err
	}

	return sqlLiteEventStore{
		ctx: ctx,
		db:  db,
	}, nil
}

func (store sqlLiteEventStore) StoreUri(containerUri string, s State) error {
	var tx *sql.Tx
	var r *sql.Rows
	var err error

	defer func() {
		if err := tx.Rollback(); err != nil && err != sql.ErrTxDone {
			log.Printf("%v", NewErrTx(rollback, containerUri, err, "persistence", "StoreUri"))
		}
	}()

	if tx, err = store.db.Begin(); err != nil {
		return NewErrTx(begin, containerUri, err, "peristence", "StoreUri")
	}

	if r, err = tx.Query(selectContainerByUri, containerUri); err != nil {
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
		if _, err = tx.Exec(updateStateByUri, s, containerUri); err != nil {
			return NewErrQuery(updateStateByUri, err, "persistence", "StoreUri", fmt.Sprintf("%d", s), containerUri)
		}
	} else {
		if _, err = tx.Exec(insertState, containerUri, s); err != nil {
			return NewErrQuery(insertState, err, "persistence", "StoreUri", containerUri, fmt.Sprintf("%d", s))
		}
	}

	if err := tx.Commit(); err != nil {
		return NewErrTx(commit, containerUri, err, "persistence", "StoreUri")
	}

	return nil
}

func (store sqlLiteEventStore) StoreContainer(c model.LdpContainer, s State) error {
	var tx *sql.Tx
	var r *sql.Rows
	var err error

	defer func() {
		if err := tx.Rollback(); err != nil && err != sql.ErrTxDone {
			log.Printf("%v", NewErrTx(rollback, c.Uri(), err, "persistence", "StoreContainer"))
		}
	}()

	if tx, err = store.db.Begin(); err != nil {
		return NewErrTx(begin, c.Uri(), err, "persistence", "StoreContainer")
	}

	if r, err = tx.Query(selectContainerByUri, c.Uri()); err != nil {
		return NewErrQuery(selectContainerByUri, err, "persistence", "StoreContainer", c.Uri())
	}

	defer func() {
		if err := r.Close(); err != nil {
			log.Printf("%v", NewErrClose(err, "persistence", "StoreContainer"))
		}
	}()

	isUpdate := r.Next()
	r.Close() // container exists

	passProperties := bytes.Buffer{}
	if err := marshalPassProperties(c, &passProperties); err != nil {
		return err
	}

	if isUpdate {
		if _, err = tx.Exec(updateContainerByUri, c.Uri(), c.Parent(), passProperties.String(), strings.Join(c.Types(), ","), s, c.Uri()); err != nil {
			return NewErrQuery(updateContainerByUri, err, "persistence", "StoreContainer", c.Uri(), c.Parent(), strings.Join(c.Contains(), ","), strings.Join(c.Types(), ","), fmt.Sprintf("%d", s), c.Uri())
		}
	} else {
		if _, err = tx.Exec(insertContainer, c.Uri(), c.Parent(), passProperties.String(), strings.Join(c.Types(), ","), s); err != nil {
			return NewErrQuery(insertContainer, err, "persistence", "StoreContainer", c.Uri(), c.Parent(), strings.Join(c.Contains(), ","), strings.Join(c.Types(), ","), fmt.Sprintf("%d", s))
		}
	}

	if err := tx.Commit(); err != nil {
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

	if r, err = store.db.Query(selectLdpcByUri, uri); err != nil {
		return model.LdpContainer{}, NewErrQuery(selectLdpcByUri, err, "persistence", "retrieveContainer", uri)
	}

	defer r.Close()

	if r.Next() {
		var (
			container, parent, pass, types []byte
		)
		if err = r.Scan(&container, &parent, &pass, &types); err != nil {
			return model.LdpContainer{}, NewErrRowScan(selectLdpcByUri, err, "persistence", "retrieveContainer", uri)
		}

		triples := []rdf.Triple{}

		containerIri, _ := rdf.NewIRI(string(container))
		parentIri, _ := rdf.NewIRI(string(parent))

		// assemble rdf:type triples
		for _, v := range strings.Split(string(types), ",") {
			subj := containerIri
			pred := rdfTypeIri
			obj, _ := rdf.NewIRI(v)
			triples = append(triples, rdf.Triple{
				Subj: rdf.Subject(subj),
				Pred: rdf.Predicate(pred),
				Obj:  rdf.Object(obj),
			})
		}

		// assemble fedora:hasParent triple
		triples = append(triples, rdf.Triple{
			Subj: containerIri,
			Pred: hasParentIri,
			Obj:  parentIri,
		})

		// assemble pass:* triples
		passProperties := map[string][]string{}
		if err = json.Unmarshal(pass, &passProperties); err != nil {
			return model.LdpContainer{},
				NewErrDeserializeContainer(err, uri, "persistence", "retrieveContainer")
		}

		for property, values := range passProperties {
			propIri, _ := rdf.NewIRI(property)

			for _, v := range values {
				if lit, err := rdf.NewLiteral(v); err != nil {
					return model.LdpContainer{},
						NewErrDeserializeContainer(err, uri, "persistence", "retrieveContainer")
				} else {
					triples = append(triples, rdf.Triple{
						Subj: containerIri,
						Pred: propIri,
						Obj:  lit,
					})
				}
			}

		}

		return model.NewContainer(triples), nil
	}

	return model.LdpContainer{}, NewErrNoResults(selectLdpcByUri, "persistence", "retrieveContainer", uri)
}

func marshalPassProperties(c model.LdpContainer, props *bytes.Buffer) error {
	if result, err := json.Marshal(c.PassProperties()); err != nil {
		return NewErrSerializeContainer(err, c.Uri(), "persistence", "marshalPassProperties")
	} else {
		props.Write(result)
	}

	return nil
}
