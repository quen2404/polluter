package polluter_test

import (
	"errors"
	"flag"
	"fmt"
	"github.com/DATA-DOG/go-txdb"
	"github.com/ory/dockertest"
	"github.com/romanyx/polluter/v2"
	"github.com/romanyx/polluter/v2/database/mongo"
	"github.com/romanyx/polluter/v2/database/mysql"
	"github.com/romanyx/polluter/v2/database/postgres"
	"github.com/romanyx/polluter/v2/database/redis"
	"github.com/romanyx/polluter/v2/internal/db_test"
	"github.com/romanyx/polluter/v2/parser"
	"github.com/romanyx/polluter/v2/parser/yaml"
	"io"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/romanyx/jwalk"
	"github.com/stretchr/testify/assert"
)

const (
	input = `users:
- id: 1
  name: Roman
- id: 2
  name: Dmitry`
	pgInput = input + `
all:
- group: first
- group: second
  `
)

type fakeEngine struct {
}

func (e fakeEngine) Build(jwalk.ObjectWalker) (polluter.Commands, error) {
	return polluter.Commands{}, nil
}

func (e fakeEngine) Exec(polluter.Commands) error {
	return nil
}

type parserFunc func(io.Reader) (jwalk.ObjectWalker, error)

func (f parserFunc) Parse(r io.Reader) (jwalk.ObjectWalker, error) {
	return f(r)
}

type dbEngineFunc func(polluter.Commands) error

func (f dbEngineFunc) Exec(cmds polluter.Commands) error {
	return f(cmds)
}

func (f dbEngineFunc) Build(jwalk.ObjectWalker) (polluter.Commands, error) {
	return polluter.Commands{
		{
			Q: "INSERT INTO",
			Args: []interface{}{
				1,
			},
		},
	}, nil
}

type objectWalker struct{}

func (o objectWalker) Walk(func(name string, value interface{}) error) error {
	return nil
}

func (o objectWalker) MarshalJSON() ([]byte, error) {
	return make([]byte, 0), nil
}

func TestMain(m *testing.M) {
	flag.Parse()

	if testing.Short() {
		os.Exit(m.Run())
	}

	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("could not connect to docker: %s\n", err)
	}

	my, err := db_test.NewMySQL(pool)
	if err != nil {
		log.Fatalf("prepare mysql with docker: %v\n", err)
	}

	txdb.Register("mysqltx", "mysql", fmt.Sprintf("test:test@tcp(localhost:%s)/test", my.Resource.GetPort("3306/tcp")))

	p, err := db_test.NewPG(pool)
	if err != nil {
		log.Fatalf("prepare pg with docker: %v\n", err)
	}

	txdb.Register("pgsqltx", "postgres", fmt.Sprintf("password=test user=test dbname=test host=localhost port=%s sslmode=disable", p.Resource.GetPort("5432/tcp")))

	r, err := db_test.NewRedis(pool)
	if err != nil {
		log.Fatalf("prepare redis with docker: %v\n", err)
	}

	mg, err := db_test.NewMongo(pool)
	if err != nil {
		log.Fatalf("prepare mongo with docker: %v\n", err)
	}

	code := m.Run()

	if err := pool.Purge(my.Resource); err != nil {
		log.Fatalf("could not purge mysql docker: %v\n", err)
	}
	if err := pool.Purge(r.Resource); err != nil {
		log.Fatalf("could not purge redis docker: %v\n", err)
	}
	if err := pool.Purge(p.Resource); err != nil {
		log.Fatalf("could not purge postgres docker: %v\n", err)
	}
	if err := pool.Purge(mg.Resource); err != nil {
		log.Fatalf("could not purge mongo docker: %v\n", err)
	}

	os.Exit(code)
}

func TestNew(t *testing.T) {
	p := polluter.New(fakeEngine{}, yaml.YAMLParser())
	err := p.Pollute(strings.NewReader(input))
	assert.Nil(t, err)
}

func Test_polluterPollute(t *testing.T) {
	tests := []struct {
		name     string
		parser   parser.Parser
		dbEngine polluter.DbEngine
		wantErr  bool
	}{
		{
			name: "parsing error",
			parser: parserFunc(func(r io.Reader) (jwalk.ObjectWalker, error) {
				return nil, errors.New("mocked error")
			}),
			wantErr: true,
		},
		{
			name: "insert error",
			parser: parserFunc(func(r io.Reader) (jwalk.ObjectWalker, error) {
				return new(objectWalker), nil
			}),
			dbEngine: dbEngineFunc(func(_ polluter.Commands) error {
				return errors.New("mocked error")
			}),
			wantErr: true,
		},
		{
			name: "without errors",
			parser: parserFunc(func(r io.Reader) (jwalk.ObjectWalker, error) {
				return new(objectWalker), nil
			}),
			dbEngine: dbEngineFunc(func(_ polluter.Commands) error {
				return nil
			}),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &polluter.Polluter{
				Parser:   tt.parser,
				DbEngine: tt.dbEngine,
			}

			err := p.Pollute(nil)

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

func TestPollute(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	tests := []struct {
		name   string
		option func(t *testing.T) (polluter.DbEngine, func() error)
		input  io.Reader
	}{
		{
			name: "mysql",
			option: func(t *testing.T) (polluter.DbEngine, func() error) {
				db, teardown := db_test.PrepareMySQLDB(t)
				return mysql.MySQLEngine(db), teardown
			},
			input: strings.NewReader(input),
		},
		{
			name: "postgres",
			option: func(t *testing.T) (polluter.DbEngine, func() error) {
				db, teardown := db_test.PreparePostgresDB(t)
				return postgres.PostgresEngine(db), teardown
			},
			input: strings.NewReader(pgInput),
		},
		{
			name: "redis",
			option: func(t *testing.T) (polluter.DbEngine, func() error) {
				db, teardown := db_test.PrepareRedisDB(t, 0)
				return redis.RedisEngine(db), teardown
			},
			input: strings.NewReader(input),
		},
		{
			name: "mongo",
			option: func(t *testing.T) (polluter.DbEngine, func() error) {
				db, teardown := db_test.PrepareMongoDB(t)
				return mongo.MongoEngine(db), teardown
			},
			input: strings.NewReader(input),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			engine, teardown := tt.option(t)
			defer func() {
				_ = teardown()
			}()

			p := polluter.New(engine, yaml.YAMLParser())
			err := p.Pollute(tt.input)
			assert.Nil(t, err)
		})
	}
}
