package txn

type Txn interface {
	Get(key ...string) string
	Put(key, val string)
	Rev(key string) int64
	Del(key string)
	commit()
	reset()
}
