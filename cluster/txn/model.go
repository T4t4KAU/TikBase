package txn

import (
	"time"
)

const (
	TxHanging TxStatus = "HANGING"
	TxSuccess TxStatus = "SUCCESS"
	TxFailure TxStatus = "FAILURE"
)

type RequestEntity struct {
	ComponentID string
	Request     map[string]any
}

type ComponentEntity struct {
	Request   map[string]any
	Component TccComponent
}

type TxStatus string

func (t TxStatus) String() string {
	return string(t)
}

const (
	TryHanging ComponentTryStatus = "HANGING"
	TrySuccess ComponentTryStatus = "SUCCESS"
	TryFailure ComponentTryStatus = "FAILURE"
)

type ComponentTryStatus string

func (c ComponentTryStatus) String() string {
	return string(c)
}

type ComponentTryEntity struct {
	ComponentID string
	Status      ComponentTryStatus
}

type Transaction struct {
	TxID       string
	Components []*ComponentTryEntity
	Status     TxStatus
	CreatedAt  time.Time
}

func NewTransaction(txId string, componentEntities []*ComponentEntity) *Transaction {
	entities := make([]*ComponentTryEntity, 0, len(componentEntities))
	for _, componentEntity := range componentEntities {
		entities = append(entities, &ComponentTryEntity{
			ComponentID: componentEntity.Component.ID(),
		})
	}

	return &Transaction{
		TxID:       txId,
		Components: entities,
	}
}

func (tx *Transaction) getStatus(createdBefore time.Time) TxStatus {
	if tx.CreatedAt.Before(createdBefore) {
		return TxFailure
	}

	var exist bool
	for _, com := range tx.Components {
		if com.Status == TryFailure {
			return TxFailure
		}
		exist = exist || (com.Status != TrySuccess)
	}

	if exist {
		return TxHanging
	}
	return TxSuccess
}
