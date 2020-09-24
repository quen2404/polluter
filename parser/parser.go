package parser

import (
	"github.com/romanyx/jwalk"
	"io"
)

type Parser interface {
	Parse(io.Reader) (jwalk.ObjectWalker, error)
}
