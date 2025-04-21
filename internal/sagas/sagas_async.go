package sagas

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/gromitlee/go-afsm/pkg/gfsm"
	"github.com/gromitlee/go-async/pkg/async/async_task"

	"github.com/gromitlee/go-dtf/internal/db/client"
	"github.com/gromitlee/go-dtf/internal/db/model"
	"github.com/gromitlee/go-dtf/internal/tools"
	"github.com/gromitlee/go-dtf/pkg/dtf_config"
	"github.com/gromitlee/go-dtf/pkg/sagas"
)

func NewSAGAS4Async(log dtf_config.Logger, client client.IDtfClient, ctx context.Context, typ uint32, iSagasFunc sagas.ISAGASFunc) async_task.ITaskFunc {
	return func() async_task.ITask {
		var wg sync.WaitGroup // 这里不用go_dtf的全局wg，在关闭go_dtf、go_async时，可以将顺序解耦
		return NewSAGAS(log, client, ctx, &wg, typ, iSagasFunc)
	}
}

// Running SAGAS异步任务实现，用于事务补偿
func (_sagas *_SAGAS) Running(ctx context.Context, stash string, stop <-chan struct{}) <-chan async_task.IReport {
	reportCh := make(chan async_task.IReport)
	_sagas.wg.Add(1)
	go func() {
		defer tools.PrintPanicStack()
		defer _sagas.wg.Done()
		defer close(reportCh)

		cancelCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		r := &report{phase: async_task.RunPhaseNormal, ctx: stash}
		defer func() {
			reportCh <- r.clone()
		}()

		// check
		_ctx, err := unmarshalCtx(stash)
		if err != nil {
			r.phase = async_task.RunPhaseFailed
			_sagas.log.Errorf("sagas %v typ %v compensate start err: %v", _sagas.uuid, _sagas.typ, err)
			return
		}

		// init _sagas
		_sagas.uuid = _ctx.Uuid
		for i, stepCtx := range _ctx.StepCtx {
			_sagas.trans[i].stepCtx = stepCtx
		}

		// compensate
		compensateCh := _sagas.compensateAFSM(cancelCtx, gfsm.State(len(_ctx.StepCtx)-1))
		for {
			select {
			case <-stop:
				// 停止compensate aFSM
				cancel()
			case _ctx = <-compensateCh:
				if _ctx != nil {
					if len(_ctx.StepCtx) == 0 {
						// compensate finish
						if err = _sagas.client.TransSAGAState(cancelCtx, _sagas.uuid, model.SAGAEventNormal); err != nil {
							_sagas.log.Errorf("sagas %v typ %v compensate finish err: %v", _sagas.uuid, _sagas.typ, err)
							r.phase = async_task.RunPhaseFailed
						} else {
							r.phase = async_task.RunPhaseFinished
						}
					} else {
						// compensate failed (maybe async task stop by sys)
						if dbCtx, err := marshalCtx(_ctx); err != nil {
							_sagas.log.Errorf("sagas %v typ %v compensate failed, marshal ctx %v err: %v", _sagas.uuid, _sagas.typ, _ctx, err)
							r.phase = async_task.RunPhaseFailed
						} else {
							r.ctx = dbCtx
						}
					}
				} else {
					r.phase = async_task.RunPhaseFailed
				}
				return
			}
		}
	}()

	return reportCh
}

// Deleting SAGAS异步任务实现(理论上不会执行Deleting)
func (_sagas *_SAGAS) Deleting(ctx context.Context, stash string, stop <-chan struct{}) <-chan async_task.IReport {
	panic("_SAGAS async task Deleting unimplemented")
}

// --- db ctx ---

// _dbCtx 用于存储序列化SAGAS事务的上下文
// 注意字段变化会导致DB数据不兼容
type _dbCtx struct {
	Uuid    string
	StepCtx []string
}

func marshalCtx(_ctx *_dbCtx) (string, error) {
	b, err := json.Marshal(_ctx)
	if err != nil {
		return "", err
	}
	return string(b), err
}

func unmarshalCtx(stash string) (*_dbCtx, error) {
	_ctx := &_dbCtx{}
	if err := json.Unmarshal([]byte(stash), _ctx); err != nil {
		return nil, err
	}
	return _ctx, nil
}

// --- report ---

// report impl IReport
type report struct {
	phase async_task.RunPhase
	ctx   string
}

func (r *report) Phase() (async_task.RunPhase, bool) {
	if r.phase == async_task.RunPhaseFinished {
		return r.phase, true
	}
	return r.phase, false
}

func (r *report) Context() string {
	return r.ctx
}

func (r *report) clone() *report {
	return &report{
		phase: r.phase,
		ctx:   r.ctx,
	}
}
