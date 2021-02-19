package persistence

import (
	"database/sql"
	"dupe-checker/model"
	"github.com/knakk/rdf"
	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"testing"
)

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

	if store, err = NewSqlLiteStore("file:/tmp/test.db?cache=shared&mode=rwc", SqliteParams{"", "", 2, 10}, nil); err != nil {
		log.Fatal(err.Error())
	}

	allTrips := decodeTriples(t, "/tmp/user-container.n3")
	ldpc := model.NewContainer(allTrips)
	log.Printf("Contains %d users", len(ldpc.Contains()))
	isPass, _ := ldpc.IsPassResource()
	assert.False(t, isPass)

	if err = store.Store(ldpc, Processed); err != nil {
		log.Fatalf("store error: %v", err)
	}

	if copy, err := store.(sqlLiteEventStore).retrieveContainer(ldpc.Uri()); err != nil {
		log.Fatalf("Error retrieving LDPC %s: %s", ldpc.Uri(), err.Error())
	} else {
		assert.Equal(t, len(ldpc.Contains()), len(copy.Contains()))
		log.Printf("Copy has %d users", len(copy.Contains()))
	}

	s, _ := rdf.NewIRI(ldpc.Uri())
	p, _ := rdf.NewIRI(model.LdpContainsUri)
	o, _ := rdf.NewIRI("http://google.com")
	allTrips = append(allTrips, rdf.Triple{
		Subj: s,
		Pred: p,
		Obj:  o,
	})

	if err = store.Store(model.NewContainer(allTrips), Processed); err != nil {
		log.Fatalf("store error: %v", err)
	}

	if copy, err := store.(sqlLiteEventStore).retrieveContainer(ldpc.Uri()); err != nil {
		log.Fatalf("Error retrieving LDPC %s: %s", ldpc.Uri(), err.Error())
	} else {
		assert.Equal(t, len(ldpc.Contains())+1, len(copy.Contains()))
		log.Printf("Copy has %d users", len(copy.Contains()))
	}

	if s, err := store.Retrieve(ldpc.Uri()); err != nil {
		log.Fatalf("store error: %v", err)
	} else {
		assert.Equal(t, Processed, s)
	}
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
