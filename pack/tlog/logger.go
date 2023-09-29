package tlog

import (
	"bytes"
	"io"
	"sync"
	"time"
)

type Logger struct {
	opt       *options
	mu        sync.Mutex
	entryPool *sync.Pool
}

type options struct {
	output        io.Writer
	level         Level
	stdLevel      Level
	formatter     Formatter
	disableCaller bool
}

type Entry struct {
	logger *Logger
	Buffer *bytes.Buffer
	Map    map[string]interface{}
	Level  Level
	Time   time.Time
	File   string
	Line   int
	Func   string
	Format string
	Args   []interface{}
}
