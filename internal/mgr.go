package internal

import (
	"context"
	"errors"
	"fmt"
	"sync"

	go_async "github.com/gromitlee/go-async"

	"github.com/gromitlee/go-dtf/internal/db/client"
	"github.com/gromitlee/go-dtf/internal/db/config"
	"github.com/gromitlee/go-dtf/internal/fixing"
	sagas2 "github.com/gromitlee/go-dtf/internal/sagas"
	"github.com/gromitlee/go-dtf/pkg/dtf_config"
	"github.com/gromitlee/go-dtf/pkg/sagas"
)

type Mgr struct {
	log    dtf_config.Logger
	client client.IDtfClient
	ctx    context.Context
	wg     *sync.WaitGroup

	cleaner fixing.IClean

	sagasTypes sync.Map // sagaTyp -> ISAGASFunc

	mutex   sync.Mutex
	stopped bool
}

func NewMgr(ctx context.Context, cfg config.Config, wg *sync.WaitGroup) (*Mgr, error) {
	// init db client
	c, err := client.NewClient(cfg.Log, cfg.DB)
	if err != nil {
		return nil, err
	}
	return &Mgr{
		log:     cfg.Log,
		client:  c,
		ctx:     ctx,
		wg:      wg,
		cleaner: fixing.NewClean(cfg.Log, c, wg),
	}, nil
}

func (m *Mgr) Run(ctx context.Context) error {
	// check stop
	if m.checkStop() {
		return errors.New("dtf mgr already stop")
	}
	// run
	m.log.Infof("dtf mgr run")
	// run components
	if err := m.cleaner.Run(ctx); err != nil {
		return fmt.Errorf("dtf mgr run clear err: %v", err)
	}
	return nil
}

func (m *Mgr) Stop() {
	m.mutex.Lock()
	// check stop
	if m.stopped {
		m.log.Errorf("dtf mgr already stop")
		m.mutex.Unlock()
		return
	}
	// stop
	m.stopped = true
	m.mutex.Unlock()
	m.cleaner.Stop()
	m.log.Infof("dtf mgr stop")
}

func (m *Mgr) RegisterSAGAS(sagasTyp uint32, newSagas sagas.ISAGASFunc) error {
	if newSagas == nil {
		return errors.New("newSagas nil")
	}
	if err := go_async.RegisterTask(sagasTyp, sagas2.NewSAGAS4Async(m.log, m.client, m.ctx, sagasTyp, newSagas)); err != nil {
		return fmt.Errorf("sagasTyp %v register in async err: %v", sagasTyp, err)
	}
	if _, exist := m.sagasTypes.LoadOrStore(sagasTyp, newSagas); exist {
		return fmt.Errorf("sagasTyp %v already registered", sagasTyp)
	}
	return nil
}

func (m *Mgr) SubmitSAGAS(ctx context.Context, sagasTyp uint32, sagasUUID, initCtx string, submitTyp sagas.SubmitType) (string, error) {
	newSagas, ok := m.sagasTypes.Load(sagasTyp)
	if !ok {
		return "", fmt.Errorf("sagasTyp %v not registered", sagasTyp)
	}
	_sagas := sagas2.NewSAGAS(m.log, m.client, m.ctx, m.wg, sagasTyp, newSagas.(sagas.ISAGASFunc))
	return _sagas.SubmitSerialize(ctx, sagasUUID, initCtx, submitTyp)
}

func (m *Mgr) checkStop() bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.stopped
}
