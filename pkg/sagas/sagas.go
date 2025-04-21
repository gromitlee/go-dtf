package sagas

import "context"

// SubmitType SAGA(S)事务提交类型
type SubmitType uint

const (
	SubmitSync  SubmitType = 0 // 同步事务：同步顺序执行各子阶段事务，事务成功/失败时返回；补偿异步执行
	SubmitAsync SubmitType = 1 // 异步事务：异步顺序执行各子阶段事务，提交后返回；事务/补偿均异步执行
)

// ISAGASFunc 用于创建序列化SAGAS或独立SAGA事务
type ISAGASFunc func() ISAGAS

// ISAGAS SAGA(S)事务，按顺序执行各子阶段事务，按逆序执行各子阶段补偿
// SAGAS事务执行过程中AP发生异常，事务失败；AP恢复正常后，继续由go-dtf负责补偿保证最终一致性
type ISAGAS interface {
	Transactions() []IStepTransaction
}

// ISAGA 独立SAGA事务，不由go_dtf事务Mgr管理
// 当执行同步事务时，Submit返回事务最后一步执行成功返回的结果；执行异步事务不返回事务执行结果
// SAGA事务执行过程中AP发生异常，事务失败无法保证最终一致性，只能人工介入
type ISAGA interface {
	Submit(ctx context.Context, uuid, initCtx string, typ SubmitType) (string, error)
}

// IStepTransaction SAGA(S)事务子阶段
type IStepTransaction interface {
	// OnTransact 子阶段事务方法
	OnTransact() Func
	// OnCompensate 子阶段补偿方法
	OnCompensate() Func
}

// Func SAGA(S)子阶段事务/补偿方法签名
// 阶段内，事务/补偿方法共用stepCtx
// 阶段事务执行成功，返回下一阶段的stepCtx，供下一阶段事务/补偿使用
type Func func(ctx context.Context, stepCtx string) (string, error)
