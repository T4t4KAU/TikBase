package errno

import "errors"

var (
	ErrKeyIsEmpty             = errors.New("the key is empty")
	ErrIndexUpdateFailed      = errors.New("failed to update index")
	ErrKeyNotFound            = errors.New("key not found in database")
	ErrDataFileNotFound       = errors.New("data file is not found")
	ErrDataDirectoryCorrupted = errors.New("the database directory maybe corrupted")
	ErrExceedCapacity         = errors.New("data exceeds capacity")
	ErrExceedMaxBatchNum      = errors.New("exceed the max write batch num")
	ErrMergeIsProgress        = errors.New("merge is in progress, try again later")
	ErrDatabaseIsUsing        = errors.New("the database directory is using")
	ErrMergeRatioUnreached    = errors.New("merge ratio is unreached")
	ErrNotEnoughDiskForMerge  = errors.New("no enough disk space for merge")
	ErrWrongTypeOperation     = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	ErrParseArgsError         = errors.New("parse args from bytes failed")
	ErrInvalidProtocol        = errors.New("invalid protocol")
)

var (
	ErrInvalidAddress = errors.New("invalid address")
)
