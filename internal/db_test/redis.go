package db_test

import (
	"fmt"
	"github.com/go-redis/redis"
	"github.com/ory/dockertest"
	"github.com/pkg/errors"
	"log"
	"testing"
	"time"
)

var (
	redisAddr = ""
)

func PrepareRedisDB(t *testing.T, db int) (cli *redis.Client, teardown func() error) {
	cli = redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: "",
		DB:       db,
	})

	_, err := cli.Ping().Result()
	if err != nil {
		log.Fatalf("ping redis: %s", err)
	}

	return cli, func() error {
		if err := cli.FlushDB().Err(); err != nil {
			return errors.Wrap(err, "flush db")
		}
		return nil
	}
}

type redisDocker struct {
	Resource *dockertest.Resource
}

func NewRedis(pool *dockertest.Pool) (*redisDocker, error) {
	res, err := pool.Run("redis", "latest", nil)
	if err != nil {
		return nil, errors.Wrap(err, "start redis")
	}

	purge := func() {
		_ = pool.Purge(res)
	}

	errChan := make(chan error)
	done := make(chan struct{})

	go func() {
		if err := pool.Retry(func() error {
			cli := redis.NewClient(&redis.Options{
				Addr: fmt.Sprintf("localhost:%s", res.GetPort("6379/tcp")),
				DB:   0,
			})
			defer cli.FlushDB()

			if _, err := cli.Ping().Result(); err != nil {
				return err
			}

			return nil
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
		return nil, errors.New("timeout on checking redis connection")
	case <-done:
		close(errChan)
	}

	redisAddr = fmt.Sprintf("localhost:%s", res.GetPort("6379/tcp"))

	r := redisDocker{
		Resource: res,
	}

	return &r, nil
}
