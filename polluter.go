package polluter

import (
	"github.com/romanyx/polluter/v2/parser"
	"io"

	"github.com/pkg/errors"
	"github.com/romanyx/jwalk"
)

type (
	Execer interface {
		Exec(Commands) error
	}

	Commands []Command

	Command struct {
		Q    string
		Args []interface{}
	}

	Builder interface {
		Build(jwalk.ObjectWalker) (Commands, error)
	}

	BuilderFct func(jwalk.ObjectWalker) (Commands, error)

	DbEngine interface {
		Builder
		Execer
	}

	// Polluter pollutes database with given input.
	Polluter struct {
		DbEngine
		parser.Parser
	}
)

// Pollute parses input from the reader and
// tries to exec generated commands on a database.
// Use New factory function to generate.
func (p *Polluter) Pollute(r io.Reader) error {
	obj, err := p.Parser.Parse(r)
	if err != nil {
		return errors.Wrap(err, "parse failed")
	}

	commands, err := p.DbEngine.Build(obj)
	if err != nil {
		return errors.Wrap(err, "Build commands failed")
	}
	if err := p.DbEngine.Exec(commands); err != nil {
		return errors.Wrap(err, "exec failed")
	}

	return nil
}

// New factory method returns initialized
// Polluter.
// For example to seed MySQL database with
// JSON input use:
//		p := New(MySQLEngine(db))
// To seed Postgres database with YAML input
// use:
// 		p := New(PostgresEngine(db), YAMLParser)
func New(engine DbEngine, parser parser.Parser) *Polluter {
	return &Polluter{
		Parser:   parser,
		DbEngine: engine,
	}
}
