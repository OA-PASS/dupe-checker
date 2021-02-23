package persistence

import (
	"database/sql"
	"dupe-checker/model"
	_ "embed"
	"github.com/knakk/rdf"
	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"testing"
)

//go:embed pass-user.n3
var userResource string

func Test_SqlLiteSimple(t *testing.T) {
	_ = &sqlite3.SQLiteDriver{}
	dsn := "file:/tmp/test.db?cache=shared&mode=rwc"
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

	if store, err = NewSqlLiteStore("file:/tmp/test3.db?cache=shared&mode=rwc", SqliteParams{"", "", 2, 10}, nil); err != nil {
		assert.Fail(t, err.Error())
	}

	// The count of expected users (i.e. number of ldp:contains triples) in our test container
	expectedUsers := 7490

	// Create a container from an n-triples representation on the filesystem (instead of contacting the repo)
	allTrips := decodeTriples(t, "/tmp/user-container.n3")
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
	//memoryDsn := "file::memory:"
	fileDsn := "file:/tmp/test3.db?cache=shared&mode=rwc"

	// Deserialize the RDF representation from an embedded resource (rather than the repository) as a model.LdpContainer
	expectedPassPropertyCount := 7
	user, err := model.ReadContainer(userResource)
	assert.Nil(t, err)
	assert.Equal(t, expectedPassPropertyCount, len(user.PassProperties()))
	expectedProperties := user.PassProperties()

	// Store the LdpContainer
	store, _ := NewSqlLiteStore(fileDsn, SqliteParams{}, nil)
	assert.Nil(t, store.StoreContainer(user, Processed))

	// Retrieve the LdpContainer from the Store
	storeCopy, _ := store.(sqlLiteEventStore).retrieveContainer(user.Uri())

	// Verify the properties
	assert.Equal(t, expectedProperties, storeCopy.PassProperties())
}

func decodeTriples(t *testing.T, file string) []rdf.Triple {
	userF, err := os.Open(file)
	assert.Nil(t, err)
	rdfDec := rdf.NewTripleDecoder(userF, rdf.NTriples)

	trip, err := rdfDec.DecodeAll()
	assert.Nil(t, err)
	assert.NotNil(t, trip)
	return trip
}
