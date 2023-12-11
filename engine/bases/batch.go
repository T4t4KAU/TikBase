package bases

import (
	"TikBase/engine/data"
	"TikBase/pack/errno"
	"TikBase/pack/utils"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

const nonTransactionSeqNo = 0

var txnFinKey = []byte("txn-fin")

type WriteBatch struct {
	options WriteBatchOptions // 配置项
	mutex   sync.RWMutex
	base    *Base

	// 暂存用户写入数据
	pending map[string]*data.LogRecord
}

func (b *Base) NewWriteBatch() *WriteBatch {
	return b.NewWriteBatchWith(DefaultWriteBatchOptions)
}

// NewWriteBatchWith 初始化WriteBatch
func (b *Base) NewWriteBatchWith(options WriteBatchOptions) *WriteBatch {
	return &WriteBatch{
		options: options,
		base:    b,
		pending: make(map[string]*data.LogRecord),
	}
}

// Put 插入数据
func (wb *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) <= 0 {
		return errno.ErrKeyIsEmpty
	}

	wb.mutex.Lock()
	defer wb.mutex.Unlock()

	rec := &data.LogRecord{Key: key, Value: value}
	wb.pending[string(key)] = rec
	return nil
}

// Delete 删除数据
func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) <= 0 {
		return errno.ErrKeyIsEmpty
	}

	wb.mutex.Lock()
	defer wb.mutex.Unlock()

	// 判断是否存在指定key
	pos := wb.base.index.Get(key)
	if pos == nil {
		// 不存在则从pending中删除
		if wb.pending[string(key)] != nil {
			delete(wb.pending, utils.B2S(key))
		}
		return nil
	}

	rec := &data.LogRecord{Key: key, Type: data.LogRecordDeleted}
	wb.pending[utils.B2S(key)] = rec // 存储pending表

	return nil
}

// Commit 事务提交 将暂存的数据写到数据文件 更新内存索引
func (wb *WriteBatch) Commit() error {
	wb.mutex.Lock()
	defer wb.mutex.Unlock()

	if len(wb.pending) <= 0 {
		return nil
	}

	// 待提交日志数量大于上限
	if uint(len(wb.pending)) > wb.options.MaxBatchNum {
		return errno.ErrExceedMaxBatchNum
	}

	// 存储引擎 加锁保证事务串行化
	wb.base.mutex.Lock()
	defer wb.base.mutex.Unlock()

	// 获取当前最新的事务序列号
	seqNo := atomic.AddUint64(&wb.base.seqNo, 1)

	// 基于pending表 追加日志
	positions := make(map[string]*data.LogRecordPos)
	for _, rec := range wb.pending {
		pos, err := wb.base.AppendLogRecord(&data.LogRecord{
			Key:   LogRecordKeyWithSeqNo(rec.Key, seqNo), // 标记序列号
			Value: rec.Value,
			Type:  rec.Type,
		})
		if err != nil {
			return err
		}

		// 记录位置信息 key -> pos
		positions[utils.B2S(rec.Key)] = pos
	}

	// 标识事务完成
	finishedRecord := &data.LogRecord{
		Key:  LogRecordKeyWithSeqNo(txnFinKey, seqNo),
		Type: data.LogRecordTxnFinished,
	}

	// 追加结束标记
	if _, err := wb.base.AppendLogRecord(finishedRecord); err != nil {
		return err
	}

	// 根据配置决定是否持久化
	if wb.options.SyncWriters && wb.base.activeFile != nil {
		if err := wb.base.activeFile.Sync(); err != nil {
			return err
		}
	}

	// 持久化已完成 二次遍历待提交日志 更新索引
	for _, rec := range wb.pending {
		key := utils.B2S(rec.Key)
		pos := positions[key] // 获取位置

		// 在索引中更新数据
		if rec.Type == data.LogRecordNormal {
			wb.base.index.Put(rec.Key, pos)
		}

		// 在索引中删除数据
		if rec.Type == data.LogRecordDeleted {
			wb.base.index.Delete(rec.Key)
		}
	}

	// 重置pending表
	wb.pending = make(map[string]*data.LogRecord)

	return nil
}

// LogRecordKeyWithSeqNo 将Key和事务号编码
func LogRecordKeyWithSeqNo(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)

	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)

	return encKey
}

// 解析LogRecord的keu 获取实际的key和事务序列号
func parseLogRecordKey(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo
}
