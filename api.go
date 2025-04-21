package go_dtf

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"gorm.io/gorm"

	"github.com/gromitlee/go-dtf/internal"
	"github.com/gromitlee/go-dtf/internal/db/config"
	sagas2 "github.com/gromitlee/go-dtf/internal/sagas"
	"github.com/gromitlee/go-dtf/internal/tools"
	"github.com/gromitlee/go-dtf/pkg/dtf_config"
	"github.com/gromitlee/go-dtf/pkg/sagas"
)

// go_dtf全局wg，事务Mgr、序列化SAGAS事务、独立SAGA事务等共用，统一管理
var _wg sync.WaitGroup

// go_dtf全局log
var _log dtf_config.Logger

// go_dtf全局ctx，异步事务、事务补偿等共用，统一管理
var _ctx context.Context
var _cancel context.CancelFunc

// go_dtf事务Mgr单例，管理序列化SAGAS事务
var _mgr *internal.Mgr

// 以上全局变量锁
var _mux sync.Mutex

// Init 初始化go_dtf
// db可选，用于是否初始化go_dtf事务Mgr，代表是否提供序列化SAGAS事务能力
func Init(ctx context.Context, options ...DTFOption) error {
	_mux.Lock()
	defer _mux.Unlock()
	// init cfg
	_log = tools.DefaultLog()
	cfg := config.Config{
		Log: _log,
		DB:  nil, // 默认不提供序列化SAGAS事务能力
	}
	for _, opt := range options {
		cfg = opt.apply(cfg)
	}
	// init ctx
	if _ctx != nil {
		return errors.New("dtf ctx already init")
	}
	_ctx, _cancel = context.WithCancel(ctx)
	// init mgr
	if cfg.DB != nil {
		var err error
		if _mgr, err = internal.NewMgr(_ctx, cfg, &_wg); err != nil {
			_ctx = nil
			return fmt.Errorf("dtf mgr init err: %v", err)
		}
		return _mgr.Run(_ctx)
	}
	return nil
}

// Stop 停止go_dtf
func Stop() {
	_mux.Lock()
	defer _mux.Unlock()
	// cancel ctx
	if _ctx != nil {
		_cancel()
		_ctx = nil
	}
	// stop mgr
	if _mgr != nil {
		_mgr.Stop()
		_mgr = nil
	}
	_wg.Wait()
}

// NewSAGA 创建独立SAGA事务
func NewSAGA(sagasTyp uint32, newSagas sagas.ISAGASFunc) (sagas.ISAGA, error) {
	_mux.Lock()
	defer _mux.Unlock()
	if _ctx == nil {
		return nil, errors.New("dtf ctx not init")
	}
	return sagas2.NewSAGAS(_log, nil, _ctx, &_wg, sagasTyp, newSagas), nil
}

// RegisterSAGAS 注册序列化SAGAS事务
func RegisterSAGAS(sagasTyp uint32, newSagas sagas.ISAGASFunc) error {
	_mux.Lock()
	defer _mux.Unlock()
	if _mgr == nil {
		return errors.New("dtf mgr not init")
	}
	return _mgr.RegisterSAGAS(sagasTyp, newSagas)
}

// SubmitSAGAS 提交序列化SAGAS事务
// 当执行同步事务时，Submit返回事务最后一步执行成功返回的结果；执行异步事务不返回事务执行结果
func SubmitSAGAS(ctx context.Context, sagasTyp uint32, sagasUUID, initCtx string, submitTyp sagas.SubmitType) (string, error) {
	_mux.Lock()
	defer _mux.Unlock()
	if _mgr == nil {
		return "", errors.New("dtf mgr not init")
	}
	return _mgr.SubmitSAGAS(ctx, sagasTyp, sagasUUID, initCtx, submitTyp)
}

// --- DTFOption ---

func WithLogger(logger dtf_config.Logger) DTFOption {
	return dtfOptionFunc(func(cfg config.Config) config.Config {
		if logger != nil {
			cfg.Log = logger
		} else {
			cfg.Log = tools.EmptyLog()
		}
		return cfg
	})
}

func WithDB(db *gorm.DB) DTFOption {
	return dtfOptionFunc(func(cfg config.Config) config.Config {
		cfg.DB = db
		return cfg
	})
}

type DTFOption interface {
	apply(cfg config.Config) config.Config
}

type dtfOptionFunc func(cfg config.Config) config.Config

func (fn dtfOptionFunc) apply(cfg config.Config) config.Config {
	return fn(cfg)
}
