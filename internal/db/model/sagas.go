package model

import "fmt"

type SAGAState int32

const (
	SAGAStateNone         SAGAState = 0
	SAGAStateTransacting  SAGAState = 1
	SAGAStateCompensating SAGAState = 2
	SAGAStateFailed       SAGAState = 3
)

type SAGAEvent int32

const (
	SAGAEventNone   SAGAEvent = 0
	SAGAEventNormal SAGAEvent = 1
	SAGAEventFailed SAGAEvent = 2
)

type DtfSaga struct {
	ID        int64 `gorm:"primary_key"`
	CreatedAt int64 `gorm:"autoCreateTime:milli;not null"`
	UpdatedAt int64 `gorm:"autoUpdateTime:milli;not null"`
	// uuid
	UUID string `gorm:"index:idx_dtf_saga_uuid;unique;not null"`
	// 类型
	Typ uint32 `gorm:"index:idx_dtf_saga_typ;not null"`
	// 状态
	State SAGAState `gorm:"index:idx_dtf_saga_state;not null"`
	// 序列化上下文
	Ctx string `gorm:"not null"`
}

func CheckTransfer(state SAGAState, event SAGAEvent) (SAGAState, bool, error) {
	switch state {
	case SAGAStateNone:
	case SAGAStateTransacting:
		switch event {
		case SAGAEventNormal:
			// 事务执行成功，删除事务记录
			return SAGAStateNone, true, nil
		case SAGAEventFailed:
			// 事务执行失败，进行事务补偿
			return SAGAStateCompensating, false, nil
		default:
		}
	case SAGAStateCompensating:
		switch event {
		case SAGAEventNormal:
			// 事务补偿成功，删除事务记录
			return SAGAStateNone, true, nil
		case SAGAEventFailed:
			// 事务补偿失败，需要人工介入
			return SAGAStateFailed, false, nil
		default:
		}
	case SAGAStateFailed:
	default:
	}
	return SAGAStateNone, false, fmt.Errorf("saga state %v event %v transfer invalid", state, event)
}
