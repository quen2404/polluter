package db_test

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/ory/dockertest"
	"github.com/pkg/errors"
	"log"
	"testing"
	"time"
)

const (
	dockerStartWait = 60 * time.Second
)

func PreparePostgresDB(t *testing.T) (db *sql.DB, teardown func() error) {
	dbName := fmt.Sprintf("db_%d", time.Now().UnixNano())
	db, err := sql.Open("pgsqltx", dbName)

	if err != nil {
		log.Fatalf("open mysql connection: %s", err)
	}

	return db, db.Close
}

type pgDocker struct {
	Resource *dockertest.Resource
}

const pgSchema = `
CREATE TABLE IF NOT EXISTS users (
	id integer NOT NULL, 
	name varchar(255) NOT NULL
);
CREATE TABLE IF NOT EXISTS "all" (
	"group" varchar(255) NOT NULL
);
`

func NewPG(pool *dockertest.Pool) (*pgDocker, error) {
	res, err := pool.Run("postgres", "latest", []string{
		"POSTGRES_PASSWORD=test",
		"POSTGRES_USER=test",
		"POSTGRES_DB=test",
	})
	if err != nil {
		return nil, errors.Wrap(err, "start postgres")
	}

	purge := func() {
		pool.Purge(res)
	}

	errChan := make(chan error)
	done := make(chan struct{})

	var db *sql.DB

	go func() {
		if err := pool.Retry(func() error {
			db, err = sql.Open("postgres", fmt.Sprintf("user=test password=test dbname=test host=localhost port=%s sslmode=disable", res.GetPort("5432/tcp")))
			if err != nil {
				return err
			}
			return db.Ping()
		}); err != nil {
			errChan <- err
		}

		close(done)
	}()

	select {
	case err := <-errChan:
		purge()
		return nil, errors.Wrap(err, "check connection")
	case <-time.After(dockerStartWait):
		purge()
		return nil, errors.New("timeout on checking postgres connection")
	case <-done:
		close(errChan)
	}

	defer db.Close()
	if _, err := db.Exec(pgSchema); err != nil {
		return nil, errors.Wrap(err, "failed to create schema")
	}

	pg := pgDocker{
		Resource: res,
	}

	return &pg, nil
}
