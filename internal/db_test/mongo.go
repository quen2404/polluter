package db_test

import (
	"context"
	"fmt"
	"github.com/ory/dockertest"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"log"
	"testing"
	"time"
)

var (
	mongoAddr = ""
)

func PrepareMongoDB(t *testing.T) (db *mongo.Database, teardown func() error) {
	dbName := fmt.Sprintf("db_%d", time.Now().UnixNano())
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoAddr))
	if err != nil {
		log.Fatalf("open mongo connection: %s", err)
	}
	return client.Database(dbName), func() error {
		return client.Disconnect(context.Background())
	}
}

type mongoDocker struct {
	Resource *dockertest.Resource
}

func NewMongo(pool *dockertest.Pool) (*mongoDocker, error) {
	res, err := pool.Run("mongo", "latest", []string{
		"MONGO_INITDB_ROOT_USERNAME=test",
		"MONGO_INITDB_ROOT_PASSWORD=test",
	})
	if err != nil {
		return nil, errors.Wrap(err, "start mongo")
	}

	purge := func() {
		pool.Purge(res)
	}

	errChan := make(chan error)
	done := make(chan struct{})

	var client *mongo.Client

	go func() {
		if err := pool.Retry(func() error {
			mongoAddr = fmt.Sprintf("mongodb://test:test@localhost:%s/admin", res.GetPort("27017/tcp"))
			client, err = mongo.Connect(context.Background(), options.Client().ApplyURI(mongoAddr))
			if err != nil {
				return err
			}
			return client.Ping(context.Background(), readpref.Primary())
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
		return nil, errors.New("timeout on checking mongo connection")
	case <-done:
		close(errChan)
	}

	defer client.Disconnect(context.Background())

	mg := mongoDocker{
		Resource: res,
	}

	return &mg, nil
}
