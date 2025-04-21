package sagas

import (
	"context"
	"fmt"
	"sync"
	"time"

	go_afsm "github.com/gromitlee/go-afsm"
	"github.com/gromitlee/go-afsm/pkg/gafsm"
	"github.com/gromitlee/go-afsm/pkg/gfsm"
	go_async "github.com/gromitlee/go-async"

	"github.com/gromitlee/go-dtf/internal/db/client"
	"github.com/gromitlee/go-dtf/internal/db/config"
	"github.com/gromitlee/go-dtf/internal/db/model"
	"github.com/gromitlee/go-dtf/internal/tools"
	"github.com/gromitlee/go-dtf/pkg/dtf_config"
	"github.com/gromitlee/go-dtf/pkg/sagas"
)

const (
	eventNext  gfsm.EventTyp = 0
	eventRetry gfsm.EventTyp = 1
)

// --- SAGA(with Serialize)  ---

// _SAGAS 统一提供序列化/独立SAGA(S)事务能力
type _SAGAS struct {
	log    dtf_config.Logger
	client client.IDtfClient
	ctx    context.Context // 异步执行子阶段事务/补偿的ctx
	wg     *sync.WaitGroup

	typ   uint32
	uuid  string // 事务uuid，同时用于判断事务是否提交
	trans []*_StepTransaction
}

func NewSAGAS(log dtf_config.Logger, client client.IDtfClient, ctx context.Context, wg *sync.WaitGroup, typ uint32, iSagasFunc sagas.ISAGASFunc) *_SAGAS {
	_sagas := &_SAGAS{
		log:    log,
		client: client,
		ctx:    ctx,
		wg:     wg,
		typ:    typ,
	}
	for idx, trans := range iSagasFunc().Transactions() {
		_sagas.trans = append(_sagas.trans, &_StepTransaction{
			sagas: _sagas,
			step:  gfsm.State(idx),
			tf:    trans.OnTransact(),
			cf:    trans.OnCompensate(),
		})
	}
	return _sagas
}

// Submit 独立SAGA事务
func (_sagas *_SAGAS) Submit(ctx context.Context, uuid, initCtx string, typ sagas.SubmitType) (string, error) {
	return _sagas.submitMod(false)(ctx, uuid, initCtx, typ)
}

// SubmitSerialize 序列化SAGAS事务
func (_sagas *_SAGAS) SubmitSerialize(ctx context.Context, uuid, initCtx string, typ sagas.SubmitType) (string, error) {
	return _sagas.submitMod(true)(ctx, uuid, initCtx, typ)
}

func (_sagas *_SAGAS) submitMod(serialize bool) func(context.Context, string, string, sagas.SubmitType) (string, error) {
	return func(ctx context.Context, uuid, initCtx string, typ sagas.SubmitType) (string, error) {
		if _sagas.uuid != "" {
			return "", fmt.Errorf("sagas %v typ %v already submit", _sagas.uuid, _sagas.typ)
		}
		_sagas.uuid = uuid
		switch typ {
		case sagas.SubmitSync:
			return _sagas.transact(ctx, initCtx, serialize)
		case sagas.SubmitAsync:
			_sagas.wg.Add(1)
			go func() {
				defer tools.PrintPanicStack()
				defer _sagas.wg.Done()
				_, _ = _sagas.transact(_sagas.ctx, initCtx, serialize)
			}()
			return "", nil
		default:
			return "", fmt.Errorf("sagas %v typ %v submitTyp %v invalid", _sagas.uuid, _sagas.typ, typ)
		}
	}
}

func (_sagas *_SAGAS) transact(ctx context.Context, initCtx string, serialize bool) (string, error) {
	_sagas.log.Debugf("sagas %v typ %v start", _sagas.uuid, _sagas.typ)

	_ctx := &_dbCtx{Uuid: _sagas.uuid}
	var dbCtx string // _ctx的json序列化
	var err error
	var done bool // transact方法是否返回
	defer func() {
		done = true
	}()

	if serialize {
		// create dbSaga
		err = _sagas.client.CreateSAGA(ctx, _sagas.uuid, _sagas.typ)
		if err != nil {
			_sagas.log.Errorf("sagas %v typ %v start err: %v", _sagas.uuid, _sagas.typ, err)
			return "", err
		}
		// heartbeat dbSaga
		_sagas.wg.Add(1)
		go func() {
			defer tools.PrintPanicStack()
			defer _sagas.wg.Done()
			ticker := time.NewTicker(time.Duration(config.SAGASHeartbeatInterval) * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-_sagas.ctx.Done():
					return
				case <-ticker.C:
					if done {
						return
					}
					if err := _sagas.client.UpdateSAGAHeartbeat(ctx, _sagas.uuid); err != nil {
						_sagas.log.Errorf("sagas %v typ %v heartbeat err: %v", _sagas.uuid, _sagas.typ, err)
					} else {
						_sagas.log.Debugf("sagas %v typ %v heartbeat", _sagas.uuid, _sagas.typ)
					}
				}
			}
		}()
	}

	// do transactions
	stepCtx := initCtx
	for _, trans := range _sagas.trans {
		_sagas.log.Debugf("sagas %v typ %v transaction %v", _sagas.uuid, _sagas.typ, trans.step)
		trans.stepCtx = stepCtx
		_ctx.StepCtx = append(_ctx.StepCtx, stepCtx)
		if serialize {
			// update dbSaga step ctx
			dbCtx, err = marshalCtx(_ctx)
			if err != nil {
				_sagas.log.Errorf("sagas %v typ %v transaction %v marshal ctx err: %v", _sagas.uuid, _sagas.typ, trans.step, err)
				_sagas.compensate(_ctx, serialize)
				return "", err
			}
			if err = _sagas.client.UpdateSAGAContext(ctx, _sagas.uuid, dbCtx); err != nil {
				_sagas.log.Errorf("sagas %v typ %v transaction %v update ctx err: %v", _sagas.uuid, _sagas.typ, trans.step, err)
				_sagas.compensate(_ctx, serialize)
				return "", err
			}
		}
		// do step transaction
		stepCtx, err = wrapperPanic(ctx, stepCtx, trans.tf)
		if err != nil {
			_sagas.log.Errorf("sagas %v typ %v transaction %v err: %v", _sagas.uuid, _sagas.typ, trans.step, err)
			_sagas.compensate(_ctx, serialize)
			return "", err
		}
	}

	if serialize {
		// dbSaga finish & delete
		if err = _sagas.client.TransSAGAState(ctx, _sagas.uuid, model.SAGAEventNormal); err != nil {
			_sagas.log.Errorf("sagas %v typ %v finish err: %v", _sagas.uuid, _sagas.typ, err)
			_sagas.compensate(_ctx, serialize)
			return "", err
		}
	}

	_sagas.log.Debugf("sagas %v typ %v finish", _sagas.uuid, _sagas.typ)
	return stepCtx, nil
}

func (_sagas *_SAGAS) compensate(_ctx *_dbCtx, serialize bool) {
	// 序列化SAGAS，创建异步任务托管事务补偿
	if serialize {
		dbCtx, err := marshalCtx(_ctx)
		if err != nil {
			_sagas.log.Errorf("sagas %v typ %v compensate marshal ctx err: %v", _sagas.uuid, _sagas.typ, err)
			return
		}
		if err = _sagas.client.TransSAGAState(_sagas.ctx, _sagas.uuid, model.SAGAEventFailed); err != nil {
			_sagas.log.Errorf("sagas %v typ %v compensate start err: %v", _sagas.uuid, _sagas.typ, err)
			return
		}
		// FIXME 如果 transacting -> compensating 后，创建异步任务前宕机，则没有进行补偿，go_dtf也无法通过fixing保证最终一致性，需要人工介入
		if _, err = go_async.CreateTask(_sagas.ctx, "", "", _sagas.typ, dbCtx, true); err != nil {
			_sagas.log.Errorf("sagas %v typ %v compensate create async task err: %v", _sagas.uuid, _sagas.typ, err)
			return
		}
		return
	}
	// 独立SAGA，异步执行事务补偿
	_sagas.wg.Add(1)
	go func() {
		defer tools.PrintPanicStack()
		defer _sagas.wg.Done()
		<-_sagas.compensateAFSM(_sagas.ctx, gfsm.State(len(_ctx.StepCtx)-1))
	}()
}

func (_sagas *_SAGAS) compensateAFSM(ctx context.Context, startStep gfsm.State) <-chan *_dbCtx {
	retChan := make(chan *_dbCtx, 1)
	_sagas.wg.Add(1)
	go func() {
		defer tools.PrintPanicStack()
		defer _sagas.wg.Done()
		defer close(retChan)
		_sagas.log.Warnf("sagas %v typ %v compensate start", _sagas.uuid, _sagas.typ)
		// afsm init
		var compensateCtx gafsm.EventCtx
		var compensateStates []gafsm.StateCfg
		var compensateTrans []gfsm.Transfer
		for idx, trans := range _sagas.trans {
			if idx == int(startStep) {
				compensateCtx = gafsm.EventCtx(trans.stepCtx)
			}
			compensateStates = append(compensateStates, gafsm.StateCfg{
				S: trans.step,
				F: trans.compensateStateFunc(),
			})
			if trans.step > 0 {
				compensateTrans = append(compensateTrans, gfsm.Transfer{
					Current:   trans.step,
					Target:    trans.step - 1,
					Condition: eventNext,
				})
			}
			compensateTrans = append(compensateTrans, gfsm.Transfer{
				Current:   trans.step,
				Target:    trans.step,
				Condition: eventRetry,
			})
		}

		aFsm, err := go_afsm.NewAFSM(compensateStates, compensateTrans, startStep, go_afsm.WithLogger(nil))
		if err != nil {
			_sagas.log.Errorf("sagas %v typ %v compensate err: %v", _sagas.uuid, _sagas.typ, err)
			return
		}
		// afsm start
		if err = aFsm.Start(ctx, compensateCtx); err != nil {
			_sagas.log.Errorf("sagas %v typ %v compensate err: %v", _sagas.uuid, _sagas.typ, err)
			return
		}
		// afsm wait
		aFsm.WaitStop()
		_ctx := &_dbCtx{Uuid: _sagas.uuid}
		if step, phase := aFsm.State(); phase != gafsm.PhaseFinished {
			_sagas.log.Errorf("sagas %v typ %v compensate %v err: phase %v maybe ctx canceled", _sagas.uuid, _sagas.typ, step, phase)
			for i, trans := range _sagas.trans {
				if i <= int(step) {
					_ctx.StepCtx = append(_ctx.StepCtx, trans.stepCtx)
				}
			}
			retChan <- _ctx
			return
		}
		_sagas.log.Warnf("sagas %v typ %v compensate finish", _sagas.uuid, _sagas.typ)
		retChan <- _ctx
	}()
	return retChan
}

// --- Step Transaction ---

type _StepTransaction struct {
	sagas   *_SAGAS
	step    gfsm.State
	stepCtx string // 该子阶段tf/cf共用
	tf      sagas.Func
	cf      sagas.Func
}

// compensateStateFunc saga.Func -> gafsm.StateFunc
func (_sagaTrans *_StepTransaction) compensateStateFunc() gafsm.StateFunc {
	return func(ctx context.Context, _ gafsm.EventCtx, _ <-chan interface{}) <-chan gafsm.Event {
		_sagaTrans.sagas.log.Warnf("sagas %v typ %v compensate %v", _sagaTrans.sagas.uuid, _sagaTrans.sagas.typ, _sagaTrans.step)
		ret := make(chan gafsm.Event, 1)
		_sagaTrans.sagas.wg.Add(1)
		go func() {
			defer tools.PrintPanicStack()
			defer _sagaTrans.sagas.wg.Done()
			defer close(ret)
			if _sagaTrans.cf != nil {
				if _, err := wrapperPanic(ctx, _sagaTrans.stepCtx, _sagaTrans.cf); err != nil {
					_sagaTrans.sagas.log.Errorf("sagas %v typ %v compensate %v err: %v", _sagaTrans.sagas.uuid, _sagaTrans.sagas.typ, _sagaTrans.step, err)
					select {
					case <-ctx.Done():
					case <-time.NewTimer(time.Duration(config.CompensateRetryInterval) * time.Second).C:
						ret <- gafsm.Event{Typ: eventRetry}
					}
					return
				}
			}
			ret <- gafsm.Event{Typ: eventNext}
		}()
		return ret
	}
}

// --- internal ---

// wrapperPanic panic -> error
func wrapperPanic(ctx context.Context, stepCtx string, f sagas.Func) (retCtx string, err error) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("%v", e)
		}
	}()
	if f != nil {
		return f(ctx, stepCtx)
	}
	return stepCtx, nil
}
