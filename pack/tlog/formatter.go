package tlog

type Formatter interface {
	Format(entry *Entry) error
}
