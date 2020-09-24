package redis_test

import (
	"bytes"
	"flag"
	"github.com/ory/dockertest"
	"github.com/romanyx/polluter/v2"
	"github.com/romanyx/polluter/v2/database/redis"
	"github.com/romanyx/polluter/v2/internal/db_test"
	"github.com/romanyx/polluter/v2/parser/json"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	flag.Parse()

	if testing.Short() {
		os.Exit(m.Run())
	}

	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("could not connect to docker: %s\n", err)
	}

	r, err := db_test.NewRedis(pool)
	if err != nil {
		log.Fatalf("prepare redis with docker: %v\n", err)
	}

	code := m.Run()

	if err := pool.Purge(r.Resource); err != nil {
		log.Fatalf("could not purge redis docker: %v\n", err)
	}

	os.Exit(code)

}

func Test_redisEngine_build(t *testing.T) {
	tests := []struct {
		name   string
		input  []byte
		expect polluter.Commands
	}{
		{
			name:  "example input",
			input: []byte(`{"count":1,"values":[1,2],"obj":{"key":"value"}}`),
			expect: polluter.Commands{
				{
					Q: "count",
					Args: []interface{}{
						[]byte(`1`),
					},
				},
				{
					Q: "values",
					Args: []interface{}{
						[]byte(`[1,2]`),
					},
				},
				{
					Q: "obj",
					Args: []interface{}{
						[]byte(`{"key":"value"}`),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			obj, err := json.JSONParser().Parse(bytes.NewReader(tt.input))
			if err != nil {
				assert.Nil(t, err)
			}

			e := redis.RedisEngine(nil)
			got, err := e.Build(obj)
			assert.Nil(t, err)
			assert.Equal(t, tt.expect, got)
		})
	}
}

func Test_redisEngine_exec(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	tests := []struct {
		name    string
		args    polluter.Commands
		wantErr bool
	}{
		{
			name: "valid query",
			args: polluter.Commands{
				{
					Q: "count",
					Args: []interface{}{
						"1",
					},
				},
			},
		},
	}

	for i, tt := range tests {
		i := i
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cli, teardown := db_test.PrepareRedisDB(t, i)
			defer func() {
				_ = teardown()
			}()
			e := redis.RedisEngine(cli)

			err := e.Exec(tt.args)

			if tt.wantErr && err == nil {
				assert.NotNil(t, err)
				return
			}

			if !tt.wantErr && err != nil {
				assert.Nil(t, err)
			}
		})
	}
}
