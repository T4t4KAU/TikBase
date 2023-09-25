package parser

type token int

const (
	GET = iota
	SET
	DEL
	EXIST
	COUNT
)
