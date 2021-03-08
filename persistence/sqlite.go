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
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/knakk/rdf"
	"github.com/mattn/go-sqlite3"
	"log"
	"strings"
	//"database/sql"
	//"database/sql/driver"
	"dupe-checker/model"
	//"github.com/mattn/go-sqlite3"
)

const (
	createContainersTable = "CREATE TABLE IF NOT EXISTS main.containers (container text UNIQUE NOT NULL, parent text, pass text, types text, state integer NOT NULL)"
	// if we need the times in this table to be processed as timestamps, we can change the type to 'numeric'
	createDupesTable     = "CREATE TABLE IF NOT EXISTS main.dupes (source text NOT NULL, target text NOT NULL, passType text NOT NULL, obverseMatchedOn text NOT NULL, obverseMatchedValues text, inverse boolean, sourceCreatedBy text, targetCreatedBy text, sourceLastModifiedBy text, targetLastModifiedBy text, sourceCreated text, targetCreated text, sourceLastModified text, targetLastModified text, UNIQUE (source, target))"
	createParentIdx      = "CREATE INDEX IF NOT EXISTS main.parent_index ON containers (parent)"
	selectContainerByUri = "SELECT container FROM main.containers WHERE container=?"
	selectStateByUri     = "SELECT state FROM main.containers WHERE container=?"
	selectLdpcByUri      = "SELECT container, parent, pass, types FROM main.containers WHERE container=?"
	//TODO inverse field
	//selectInverseDupe    = "SELECT source,target FROM main.dupes WHERE source=? and target=?"
	updateStateByUri      = "UPDATE main.containers SET state = ? WHERE container = ?"
	updateContainerByUri  = "UPDATE main.containers SET container = ?, parent = ?, pass = ?, types = ?, state = ? WHERE container = ?"
	insertState           = "INSERT INTO main.containers (container, state) VALUES (?, ?)"
	insertContainer       = "INSERT INTO main.containers (container, parent, pass, types, state) VALUES (?, ?, ?, ?, ?)"
	insertDupe            = "INSERT INTO main.dupes (source, target, passType, obverseMatchedOn, obverseMatchedValues, sourceCreatedBy, targetCreatedBy, sourceLastModifiedBy, targetLastModifiedBy, sourceCreated, targetCreated, sourceLastModified, targetLastModified) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	updateDupeWithInverse = "UPDATE main.dupes SET inverse = true, targetCreatedBy = ?, targetCreated = ?, targetLastModified = ?, targetLastModifiedBy = ? WHERE source = ? AND target = ?"
	selectDupeForInverse  = "SELECT 1 from main.dupes where source = ? AND target = ?"
	selectCountFromDupes  = "SELECT count(*) from main.dupes"
	selectTargetFromSource = "SELECT target from main.dupes where source = ?"
	selectSourceFromTarget = "SELECT source from main.dupes where target = ?"
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

	//sqlite3conn := []*sqlite3.SQLiteConn{}
	//sql.Register("sqlite3_with_hook_example",
	//	&sqlite3.SQLiteDriver{
	//		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
	//			sqlite3conn = append(sqlite3conn, conn)
	//			conn.RegisterCommitHook(func() int {
	//				log.Println("Executing commit")
	//				return 0
	//			})
	//			conn.RegisterRollbackHook(func() {
	//				log.Println("Executing rollback")
	//			})
	//			conn.RegisterUpdateHook(func(op int, db string, table string, rowid int64) {
	//				switch op {
	//				case sqlite3.SQLITE_INSERT:
	//					log.Println("Notified of insert on db", db, "table", table, "rowid", rowid)
	//				case sqlite3.SQLITE_SELECT:
	//					log.Println("Notified of select on db", db, "table", table, "rowid", rowid)
	//				case sqlite3.SQLITE_UPDATE:
	//					log.Println("Notified of update on db", db, "table", table, "rowid", rowid)
	//				}
	//			})
	//			return nil
	//		},
	//	})

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

	_, err = db.Exec(createDupesTable)

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

func (store sqlLiteEventStore) StoreDupe(source, target, passType string, obverseMatchedOn, obverseMatchedValues []string, attribs DupeContainerAttributes) error {
	var tx *sql.Tx
	var err error

	defer func() {
		if err := tx.Rollback(); err != nil && err != sql.ErrTxDone {
			log.Printf("%v", NewErrTx(rollback, fmt.Sprintf("dupe: <%s> <%s>", source, target), err, "persistence", "StoreDupe"))
		}
	}()

	if tx, err = store.db.Begin(); err != nil {
		return NewErrTx(begin, fmt.Sprintf("dupe: <%s> <%s>", source, target), err, "persistence", "StoreDupe")
	}

	// Check if the inverse exists, if so, update the existing row and return
	if inverseExists, err := hasInverse(tx, source, target); err != nil {
		return err
	} else if inverseExists {

		// set the inverse flag to true, and update the *target* in the database with the *source* from the match
		// (since we are processing an inverse)
		if _, err := tx.Exec(updateDupeWithInverse, attribs.SourceCreatedBy, attribs.SourceCreated,
			attribs.SourceLastModified, attribs.SourceLastModifiedBy, target, source); err != nil {
			return NewErrQuery(updateDupeWithInverse, err, "persistence", "StoreDupe",
				attribs.SourceCreatedBy, attribs.SourceCreated.String(), attribs.SourceLastModified.String(),
				attribs.SourceLastModifiedBy, target, source)
		}

		return commitTx(source, target, tx)
	}

	// If the inverse doesn't exist, we have a new dupe to insert
	if _, err = tx.Exec(insertDupe, source, target, passType, strings.Join(obverseMatchedOn, ","),
		strings.Join(obverseMatchedValues, ";"), attribs.SourceCreatedBy, attribs.TargetCreatedBy,
		attribs.SourceLastModifiedBy, attribs.TargetLastModifiedBy, attribs.SourceCreated, attribs.TargetCreated,
		attribs.SourceLastModified, attribs.TargetLastModified); err != nil {

		if sqliteErr, ok := err.(sqlite3.Error); ok {
			if sqliteErr.Code == sqlite3.ErrConstraint {
				return NewErrConstraint(insertDupe, err, "persistence", "StoreDupe", source, target, passType,
					strings.Join(obverseMatchedOn, ","), strings.Join(obverseMatchedValues, ";"),
					attribs.SourceCreatedBy, attribs.TargetCreatedBy, attribs.SourceLastModifiedBy,
					attribs.TargetLastModifiedBy, attribs.SourceCreated.String(), attribs.TargetCreated.String(),
					attribs.SourceLastModified.String(), attribs.TargetLastModified.String())
			}
		}

		return NewErrQuery(insertDupe, err, "persistence", "StoreDupe", source, target, passType,
			strings.Join(obverseMatchedOn, ","), strings.Join(obverseMatchedValues, ";"),
			attribs.SourceCreatedBy, attribs.TargetCreatedBy, attribs.SourceLastModifiedBy,
			attribs.TargetLastModifiedBy, attribs.SourceCreated.String(), attribs.TargetCreated.String(),
			attribs.SourceLastModified.String(), attribs.TargetLastModified.String())
	}

	return commitTx(source, target, tx)
}

func (store sqlLiteEventStore) ExpandValue(value string) ([]string, error) {
	var r *sql.Rows
	var err error
	var result []string

	if r, err = store.db.Query(selectSourceFromTarget, value); err != nil {
		return result, NewErrQuery(selectSourceFromTarget, err, "persistence", "ExpandValue", value)
	}

	defer r.Close()

	v := ""
	for r.Next() {
		if err = r.Scan(&v); err != nil {
			return result, NewErrRowScan(selectSourceFromTarget, err, "persistence", "ExpandValue", value)
		} else {
			result = append(result, v)
		}
	}

	r.Close()

	if r, err = store.db.Query(selectTargetFromSource, value); err != nil {
		return result, NewErrQuery(selectTargetFromSource, err, "persistence", "ExpandValue", value)
	}

	for r.Next() {
		if err = r.Scan(&v); err != nil {
			return result, NewErrRowScan(selectTargetFromSource, err, "persistence", "ExpandValue", value)
		} else {
			result = append(result, v)
		}
	}

	return result, nil
}

func commitTx(source string, target string, tx *sql.Tx) error {
	if err := tx.Commit(); err != nil {
		return NewErrTx(commit, fmt.Sprintf("dupe: <%s> <%s>", source, target), err, "persistence", "StoreDupe")
	}
	return nil
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

func hasInverse(tx *sql.Tx, source, target string) (bool, error) {
	var res int
	if err := tx.QueryRow(selectDupeForInverse, target, source).Scan(&res); err != nil {
		if err != sql.ErrNoRows {
			return false, NewErrQuery(selectDupeForInverse, err, "persistence", "StoreDupe", source, target)
		}
		return false, nil
	}
	return true, nil
}

func marshalPassProperties(c model.LdpContainer, props *bytes.Buffer) error {
	if result, err := json.Marshal(c.PassProperties()); err != nil {
		return NewErrSerializeContainer(err, c.Uri(), "persistence", "marshalPassProperties")
	} else {
		props.Write(result)
	}

	return nil
}

func newErrConstraint(source string, target string, passType string, obverseMatchedOn []string, obverseMatchedValues []string, attribs DupeContainerAttributes, err error) StoreErr {
	return NewErrConstraint(insertDupe, err, "persistence", "StoreDupe", source, target, passType,
		strings.Join(obverseMatchedOn, ","), strings.Join(obverseMatchedValues, ";"), attribs.SourceCreatedBy,
		attribs.TargetCreatedBy, attribs.SourceLastModifiedBy, attribs.TargetLastModifiedBy,
		attribs.SourceCreated.String(), attribs.TargetCreated.String(), attribs.SourceLastModified.String(),
		attribs.TargetLastModified.String())
}

func newErrQuery(source string, target string, passType string, obverseMatchedOn []string, obverseMatchedValues []string, attribs DupeContainerAttributes, err error) StoreErr {
	return NewErrQuery(insertDupe, err, "persistence", "StoreDupe", source, target, passType,
		strings.Join(obverseMatchedOn, ","), strings.Join(obverseMatchedValues, ";"),
		attribs.SourceCreatedBy, attribs.TargetCreatedBy,
		attribs.SourceLastModifiedBy, attribs.TargetLastModifiedBy, attribs.SourceCreated.String(),
		attribs.TargetCreated.String(), attribs.SourceLastModified.String(), attribs.TargetLastModified.String())
}
