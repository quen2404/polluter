package db_test

import (
	"database/sql"
	"fmt"
	mysqlD "github.com/go-sql-driver/mysql"
	"github.com/ory/dockertest"
	"github.com/pkg/errors"
	"io/ioutil"
	"log"
	"testing"
	"time"
)

func PrepareMySQLDB(t *testing.T) (db *sql.DB, teardown func() error) {
	dbName := fmt.Sprintf("db_%d", time.Now().UnixNano())
	db, err := sql.Open("mysqltx", dbName)

	if err != nil {
		log.Fatalf("open mysql connection: %s", err)
	}

	return db, db.Close
}

type mySQL struct {
	Resource *dockertest.Resource
}

const mysqlSchema = `
CREATE TABLE IF NOT EXISTS users (
	id integer NOT NULL,
	name varchar(255) NOT NULL
);
`

func NewMySQL(pool *dockertest.Pool) (*mySQL, error) {
	res, err := pool.Run("mysql", "latest", []string{
		"MYSQL_ROOT_PASSWORD=qwerty",
		"MYSQL_USER=test",
		"MYSQL_PASSWORD=test",
		"MYSQL_DATABASE=test",
	})
	if err != nil {
		return nil, errors.Wrap(err, "start mysql")
	}

	purge := func() {
		pool.Purge(res)
	}

	errChan := make(chan error)
	done := make(chan struct{})

	mysqlD.SetLogger(log.New(ioutil.Discard, "", 0)) // mute mysql logger.

	var db *sql.DB

	go func() {
		if err := pool.Retry(func() error {
			db, err = sql.Open("mysql", fmt.Sprintf("test:test@(localhost:%s)/test", res.GetPort("3306/tcp")))
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
		return nil, errors.New("timeout on checking mysql connection")
	case <-done:
		close(errChan)
	}

	defer db.Close()
	if _, err := db.Exec(mysqlSchema); err != nil {
		return nil, errors.Wrap(err, "failed to create schema")
	}

	mysql := mySQL{
		Resource: res,
	}

	return &mysql, nil
}
