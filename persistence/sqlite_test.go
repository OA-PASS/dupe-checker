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
	"database/sql"
	"dupe-checker/model"
	_ "embed"
	"errors"
	"github.com/knakk/rdf"
	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"strings"
	"testing"
	"time"
)

//go:embed pass-user.n3
var userResource string

//go:embed pass-user-container.n3
var userContainer string

func Test_SqlLiteSimple(t *testing.T) {
	_ = &sqlite3.SQLiteDriver{}
	dsn := ":memory:"
	//sqlDriver := sqlite3.SQLiteDriver{
	//	Extensions:  nil,
	//	ConnectHook: nil,
	//}

	var db *sql.DB
	var res sql.Result
	var rows *sql.Rows
	var err error

	if db, err = sql.Open("sqlite3", dsn); err != nil {
		log.Fatal(err.Error())
	}

	db.SetMaxIdleConns(2)
	db.SetMaxOpenConns(5)

	if err = db.Ping(); err != nil {
		log.Fatal(err.Error())
	}

	res, err = db.Exec(`
CREATE TABLE IF NOT EXISTS main.containers (container text UNIQUE NOT NULL, parent text, contains text, types text NOT NULL, state integer NOT NULL)
`)

	if err != nil {
		log.Fatalf(err.Error())
	}

	if i, err := res.RowsAffected(); err != nil {
		log.Fatalf(err.Error())
	} else {
		log.Printf("%d rows affected", i)
	}

	res, err = db.Exec(`
CREATE INDEX IF NOT EXISTS main.parent_index ON containers (parent)
`)

	if err != nil {
		log.Fatalf(err.Error())
	}

	if i, err := res.RowsAffected(); err != nil {
		log.Fatalf(err.Error())
	} else {
		log.Printf("%d rows affected", i)
	}

	rows, err = db.Query("SELECT name FROM sqlite_schema") //" WHERE type = 'table'")

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
}

func Test_DriverSimple(t *testing.T) {
	var store Store
	var err error

	if store, err = NewSqlLiteStore(":memory:", SqliteParams{"", "", 2, 10}, nil); err != nil {
		assert.Fail(t, err.Error())
	}

	// The count of expected users (i.e. number of ldp:contains triples) in our test container
	expectedUsers := 7490

	// Create a container from an n-triples representation on the filesystem (instead of contacting the repo)
	allTrips := decodeTriples(t, userContainer)
	ldpc := model.NewContainer(allTrips)
	assert.Equal(t, expectedUsers, len(ldpc.Contains()))
	isPass, _ := ldpc.IsPassResource()
	assert.False(t, isPass)

	// Store the container in the DB.
	assert.Nil(t, store.StoreContainer(ldpc, Processed))

	// Retrieve the container from the database.  Note ldp:contains relationships are *not* present.
	if dbCopy, err := store.(sqlLiteEventStore).retrieveContainer(ldpc.Uri()); err != nil {
		assert.Fail(t, err.Error())
	} else {
		assert.NotEqual(t, len(ldpc.Contains()), len(dbCopy.Contains()))
		log.Printf("Repo has %d users, db copy has %d users", len(ldpc.Contains()), len(dbCopy.Contains()))
	}

	// Attempts to add an ldp:contains relationship and store it will fail, since ldp:contains are not persisted as such
	// in the DB.
	s, _ := rdf.NewIRI(ldpc.Uri())
	p, _ := rdf.NewIRI(model.LdpContainsUri)
	o, _ := rdf.NewIRI("http://google.com")
	allTrips = append(allTrips, rdf.Triple{
		Subj: s,
		Pred: p,
		Obj:  o,
	})

	// Store the container with the additional triple (that won't be persisted) and update its state (which will be persisted)
	assert.Nil(t, store.StoreContainer(model.NewContainer(allTrips), Processed))

	// Containment triples not stored in the copy
	if dbCopy, err := store.(sqlLiteEventStore).retrieveContainer(ldpc.Uri()); err != nil {
		assert.Fail(t, err.Error())
	} else {
		assert.Equal(t, 0, len(dbCopy.Contains()))
	}

	// But state is updated
	if s, err := store.Retrieve(ldpc.Uri()); err != nil {
		assert.Fail(t, err.Error())
	} else {
		assert.Equal(t, Processed, s)
	}
}

func Test_RoundTripProperties(t *testing.T) {
	memoryDsn := ":memory:"

	// Deserialize the RDF representation from an embedded resource (rather than the repository) as a model.LdpContainer
	expectedPassPropertyCount := 7
	user, err := model.ReadContainer(userResource)
	assert.Nil(t, err)
	assert.Equal(t, expectedPassPropertyCount, len(user.PassProperties()))
	expectedProperties := user.PassProperties()

	// Store the LdpContainer
	store, _ := NewSqlLiteStore(memoryDsn, SqliteParams{}, nil)
	assert.Nil(t, store.StoreContainer(user, Processed))

	// Retrieve the LdpContainer from the Store
	storeCopy, _ := store.(sqlLiteEventStore).retrieveContainer(user.Uri())

	// Verify the properties
	assert.Equal(t, expectedProperties, storeCopy.PassProperties())
}

func Test_StoreDupeConstraint(t *testing.T) {
	dsn := ":memory:"
	//dsn := "file:/tmp/storedupeconstraint.db"
	store, err := NewSqlLiteStore(dsn, SqliteParams{}, nil)
	assert.Nil(t, err)

	err = store.StoreDupe("source", "target", "Publication", []string{"doi"}, []string{"10.5860/crln.78.1.9605"}, DupeContainerAttributes{})
	assert.Nil(t, err)

	err = store.StoreDupe("source", "target", "Publication", []string{"doi"}, []string{"10.5860/crln.78.1.9605"}, DupeContainerAttributes{})
	assert.NotNil(t, err)
	assert.True(t, errors.Is(err, ErrConstraint))

	// when storing the inverse, the type and matchesOn are ignored in the sense that they don't get written to the database.
	// old behavior was that a new row would be created which contains the type and matchesOn
	// consequence is that the the matchesOn field is only valid for the obverse relationship.  it is possible that the
	// inverse relationship matched on something else.
	err = store.StoreDupe("target", "source", "Publication", []string{"doi"}, []string{"10.5860/crln.78.1.9605"}, DupeContainerAttributes{})
	assert.Nil(t, err)

	// FIXME note that attempting to store the inverse of a dupe multiple times does not raise a constraint violation
	// This means that the target container attributes in the row is overwritten but otherwise info remains the same.
	err = store.StoreDupe("target", "source", "Publication", []string{"doi"}, []string{"10.5860/crln.78.1.9605"}, DupeContainerAttributes{})
	assert.Nil(t, err)
}

func Test_StoreDupeInverse(t *testing.T) {
	dsn := ":memory:"
	//dsn := "file:/tmp/storedupeconstraint.db"
	var store Store
	var err error

	if store, err = NewSqlLiteStore(dsn, SqliteParams{"", "", 2, 10}, nil); err != nil {
		assert.Fail(t, err.Error())
	}

	// nothing in the DB, hasInverse for {source,target}, and hasInverse for {target,source} should return false
	if tx, err := store.(sqlLiteEventStore).db.Begin(); err != nil {
		assert.Fail(t, err.Error())
	} else {
		if hasInverse, err := hasInverse(tx, "source", "target"); err != nil {
			assert.Fail(t, err.Error())
		} else {
			assert.False(t, hasInverse)
		}
		if hasInverse, err := hasInverse(tx, "target", "source"); err != nil {
			assert.Fail(t, err.Error())
		} else {
			assert.False(t, hasInverse)
		}
		tx.Commit()
	}

	// insert the obverse dupe
	err = store.StoreDupe("source", "target", "Publication", []string{"nlmta"}, []string{"Jor Nat"}, DupeContainerAttributes{
		SourceCreatedBy:      "obverse sourceCreatedBy",
		SourceLastModifiedBy: "obverse sourceLastModifiedBy",
		SourceCreated:        time.Now(),
		SourceLastModified:   time.Now(),
	})

	// one row in the DB, hasInverse for {target, source} should return true

	if tx, err := store.(sqlLiteEventStore).db.Begin(); err != nil {
		assert.Fail(t, err.Error())
	} else {
		if hasInverse, err := hasInverse(tx, "target", "source"); err != nil {
			assert.Fail(t, err.Error())
		} else {
			assert.True(t, hasInverse)
		}
		tx.Commit()
	}

	// insert the inverse dupe.  This should not create a new row, instead the inverse flag should be flipped
	err = store.StoreDupe("target", "source", "Publication", []string{"nlmta"}, []string{"Jor Nat"}, DupeContainerAttributes{
		SourceCreatedBy:      "inverse sourceCreatedBy",
		SourceLastModifiedBy: "inverse sourceLastModifiedBy",
		SourceCreated:        time.Now(),
		SourceLastModified:   time.Now(),
	})

	assert.Nil(t, err)

	var res int
	var inv bool
	assert.Nil(t, store.(sqlLiteEventStore).db.QueryRow("select count(*) from main.dupes").Scan(&res))
	assert.Equal(t, 1, res)
	assert.Nil(t, store.(sqlLiteEventStore).db.
		QueryRow("select inverse from main.dupes WHERE source = ? AND target = ?", "source", "target").Scan(&inv))
	assert.True(t, inv)

	// one row in the db, inverse flag = true.  hasInverse{target, source} should return true

	if tx, err := store.(sqlLiteEventStore).db.Begin(); err != nil {
		assert.Fail(t, err.Error())
	} else {

		if hasInverse, err := hasInverse(tx, "target", "source"); err != nil {
			assert.Fail(t, err.Error())
		} else {
			assert.True(t, hasInverse)
		}
		tx.Commit()
	}
}

func Test_ExpandValueWithTarget(t *testing.T) {
	dsn := ":memory:"
	var store Store
	var err error
	var result []string

	if store, err = NewSqlLiteStore(dsn, SqliteParams{"", "", 2, 10}, nil); err != nil {
		assert.Fail(t, err.Error())
	}

	source := "source"
	dupes := []string{"dupeA", "dupeB", "dupeC"}

	for i := range dupes {
		store.StoreDupe(source, dupes[i], "type", []string{"matchedOn"}, []string{"matchedValue"}, DupeContainerAttributes{})
	}

	result, err = store.ExpandValue(source)
	assert.Nil(t, err)
	assert.EqualValues(t, dupes, result)
}

func Test_ExpandValueWithSource(t *testing.T) {
	dsn := ":memory:"
	var store Store
	var err error
	var result []string

	if store, err = NewSqlLiteStore(dsn, SqliteParams{"", "", 2, 10}, nil); err != nil {
		assert.Fail(t, err.Error())
	}

	target := "source"
	dupes := []string{"dupeA", "dupeB", "dupeC"}

	for i := range dupes {
		store.StoreDupe(dupes[i], target, "type", []string{"matchedOn"}, []string{"matchedValue"}, DupeContainerAttributes{})
	}

	result, err = store.ExpandValue(target)
	assert.Nil(t, err)
	assert.EqualValues(t, dupes, result)
}

func Test_ExpandValueBidirectional(t *testing.T) {
	dsn := ":memory:"
	var store Store
	var err error
	var result []string

	if store, err = NewSqlLiteStore(dsn, SqliteParams{"", "", 2, 10}, nil); err != nil {
		assert.Fail(t, err.Error())
	}

	value := "target"

	sourceDupes := []string{"sourceA", "sourceB", "sourceC"}
	for i := range sourceDupes {
		store.StoreDupe(sourceDupes[i], value, "type", []string{"matchedOn"}, []string{"matchedValue"}, DupeContainerAttributes{})
	}

	targetDupes := []string{"targetA", "targetB", "targetC"}
	for i := range targetDupes {
		store.StoreDupe(value, targetDupes[i], "type", []string{"matchedOn"}, []string{"matchedValue"}, DupeContainerAttributes{})
	}

	result, err = store.ExpandValue(value)
	assert.Nil(t, err)
	assert.EqualValues(t, append(sourceDupes, targetDupes...), result)
}

// ExpandValue should return the original value, even if the value is not expanded.
func Test_ExpandValueNoExpansion(t *testing.T) {
	dsn := ":memory:"
	var store Store
	var err error
	var result []string

	if store, err = NewSqlLiteStore(dsn, SqliteParams{"", "", 2, 10}, nil); err != nil {
		assert.Fail(t, err.Error())
	}

	target := "source"
	dupes := []string{"value"}
	store.StoreDupe(dupes[0], target, "type", []string{"matchedOn"}, []string{"matchedValue"}, DupeContainerAttributes{})

	result, err = store.ExpandValue(target)
	assert.Nil(t, err)
	assert.EqualValues(t, dupes, result)
}

// ExpandValue should return the original value, even if a duplicate isn't found
func Test_ExpandValueNoExpansionNoDupe(t *testing.T) {
	dsn := ":memory:"
	var store Store
	var err error
	var result []string

	if store, err = NewSqlLiteStore(dsn, SqliteParams{"", "", 2, 10}, nil); err != nil {
		assert.Fail(t, err.Error())
	}

	target := "source"

	result, err = store.ExpandValue(target)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(result))
	assert.EqualValues(t, target, result[0])
}

func Test_ConnhookTest(t *testing.T) {
	sqlite3conn := []*sqlite3.SQLiteConn{}
	sql.Register("sqlite3_with_hook_example",
		&sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				sqlite3conn = append(sqlite3conn, conn)
				conn.RegisterUpdateHook(func(op int, db string, table string, rowid int64) {
					switch op {
					case sqlite3.SQLITE_INSERT:
						log.Println("Notified of insert on db", db, "table", table, "rowid", rowid)
					}
				})
				return nil
			},
		})
	os.Remove("./foo.db")
	os.Remove("./bar.db")

	srcDb, err := sql.Open("sqlite3_with_hook_example", "./foo.db")
	if err != nil {
		log.Fatal(err)
	}
	defer srcDb.Close()
	srcDb.Ping()

	_, err = srcDb.Exec("create table foo(id int, value text)")
	if err != nil {
		log.Fatal(err)
	}
	_, err = srcDb.Exec("insert into foo values(1, 'foo')")
	if err != nil {
		log.Fatal(err)
	}
	_, err = srcDb.Exec("insert into foo values(2, 'bar')")
	if err != nil {
		log.Fatal(err)
	}
	_, err = srcDb.Query("select * from foo")
	if err != nil {
		log.Fatal(err)
	}
	destDb, err := sql.Open("sqlite3_with_hook_example", "./bar.db")
	if err != nil {
		log.Fatal(err)
	}
	defer destDb.Close()
	destDb.Ping()

	bk, err := sqlite3conn[1].Backup("main", sqlite3conn[0], "main")
	if err != nil {
		log.Fatal(err)
	}

	_, err = bk.Step(-1)
	if err != nil {
		log.Fatal(err)
	}
	_, err = destDb.Query("select * from foo")
	if err != nil {
		log.Fatal(err)
	}
	_, err = destDb.Exec("insert into foo values(3, 'bar')")
	if err != nil {
		log.Fatal(err)
	}

	bk.Finish()
}

func decodeTriples(t *testing.T, n3Resource string) []rdf.Triple {
	rdfDec := rdf.NewTripleDecoder(strings.NewReader(n3Resource), rdf.NTriples)
	trip, err := rdfDec.DecodeAll()
	assert.Nil(t, err)
	assert.NotNil(t, trip)
	return trip
}
