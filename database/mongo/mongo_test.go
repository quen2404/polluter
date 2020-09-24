package mongo_test

import (
	"bytes"
	"flag"
	"github.com/romanyx/polluter/v2/internal/db_test"
	"log"
	"os"
	"testing"

	"github.com/ory/dockertest"
	"github.com/romanyx/polluter/v2"
	"github.com/romanyx/polluter/v2/database/mongo"
	"github.com/romanyx/polluter/v2/parser/json"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
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

	mg, err := db_test.NewMongo(pool)
	if err != nil {
		log.Fatalf("prepare mongo with docker: %v\n", err)
	}

	code := m.Run()

	if err := pool.Purge(mg.Resource); err != nil {
		log.Fatalf("could not purge mongo docker: %v\n", err)
	}

	os.Exit(code)

}

func Test_mongoEngineBuild(t *testing.T) {
	tests := []struct {
		name   string
		input  []byte
		expect polluter.Commands
	}{
		{
			name:  "example input",
			input: []byte(`{"users":[{"id":1,"name":"Roman"},{"id":2,"name":"Dmitry"}],"roles":[{"id":2,"role_ids":[1,2]}]}`),
			expect: polluter.Commands{
				{
					Q: "users",
					Args: []interface{}{
						bson.D{
							bson.E{
								Key:   "id",
								Value: int32(1),
							},
							bson.E{
								Key:   "name",
								Value: "Roman",
							},
						},
						bson.D{
							bson.E{
								Key:   "id",
								Value: int32(2),
							},
							bson.E{
								Key:   "name",
								Value: "Dmitry",
							},
						},
					},
				},
				{
					Q: "roles",
					Args: []interface{}{
						bson.D{
							bson.E{
								Key:   "id",
								Value: int32(2),
							},
							bson.E{
								Key:   "role_ids",
								Value: bson.A{int32(1), int32(2)},
							},
						},
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

			e := mongo.MongoEngine(nil)
			got, err := e.Build(obj)
			assert.Nil(t, err)
			assert.Equal(t, tt.expect, got)
		})
	}
}

func Test_mongoEngine_exec(t *testing.T) {
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
			args: polluter.Commands{},
		}, {
			name: "invalid query",
			args: polluter.Commands{},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db, teardown := db_test.PrepareMongoDB(t)
			defer func() {
				_ = teardown()
			}()
			e := mongo.MongoEngine(db)

			err := e.Exec(tt.args)

			if tt.wantErr && err != nil {
				assert.NotNil(t, err)
				return
			}

			if !tt.wantErr && err != nil {
				assert.Nil(t, err)
			}
		})
	}
}
