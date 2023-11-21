package errorx

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
)
