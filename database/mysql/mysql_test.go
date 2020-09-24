package mysql_test

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/DATA-DOG/go-txdb"
	"github.com/ory/dockertest"
	"github.com/quen2404/polluter"
	"github.com/quen2404/polluter/database/mysql"
	"github.com/quen2404/polluter/internal/db_test"
	"github.com/quen2404/polluter/parser/json"
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

	my, err := db_test.NewMySQL(pool)
	if err != nil {
		log.Fatalf("prepare mysql with docker: %v\n", err)
	}

	txdb.Register("mysqltx", "mysql", fmt.Sprintf("test:test@tcp(localhost:%s)/test", my.Resource.GetPort("3306/tcp")))

	code := m.Run()

	if err := pool.Purge(my.Resource); err != nil {
		log.Fatalf("could not purge mysql docker: %v\n", err)
	}

	os.Exit(code)

}

func Test_mysqlEngine_build(t *testing.T) {
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
					Q: "INSERT INTO `users` (`id`, `name`) VALUES (?, ?);",
					Args: []interface{}{
						float64(1),
						"Roman",
					},
				},
				{
					Q: "INSERT INTO `users` (`id`, `name`) VALUES (?, ?);",
					Args: []interface{}{
						float64(2),
						"Dmitry",
					},
				},
				{
					Q: "INSERT INTO `roles` (`id`, `role_ids`) VALUES (?, ?);",
					Args: []interface{}{
						float64(2),
						[]interface{}{
							float64(1),
							float64(2),
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

			e := mysql.MySQLEngine(nil)
			got, err := e.Build(obj)
			assert.Nil(t, err)
			assert.Equal(t, tt.expect, got)
		})
	}
}

func Test_mysqlEngine_exec(t *testing.T) {
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
					Q: "INSERT INTO `users` (`id`, `name`) VALUES (?, ?);",
					Args: []interface{}{
						1,
						"Roman",
					},
				},
			},
		},
		{
			name: "invalid query",
			args: polluter.Commands{
				{
					Q: "INSERT INTO `roles` (`id`, `name`) VALUES (?, ?);",
					Args: []interface{}{
						1,
						"User",
					},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, teardown := db_test.PrepareMySQLDB(t)
			defer func() {
				_ = teardown()
			}()
			e := mysql.MySQLEngine(db)

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
