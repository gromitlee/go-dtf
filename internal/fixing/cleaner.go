package fixing

import (
	"context"
	"errors"
	"sync"
	"time"

	go_async "github.com/gromitlee/go-async"

	"github.com/gromitlee/go-dtf/internal/db/client"
	"github.com/gromitlee/go-dtf/internal/db/config"
	"github.com/gromitlee/go-dtf/internal/tools"
	"github.com/gromitlee/go-dtf/pkg/dtf_config"
)

// IClean fix事务执行中但AP发生异常的事务
type IClean interface {
	Run(ctx context.Context) error
	Stop()
}

type cleaner struct {
	log    dtf_config.Logger
	client client.IDtfClient
	wg     *sync.WaitGroup

	mutex   sync.Mutex
	stopped bool
	stop    chan struct{}
}

func NewClean(log dtf_config.Logger, c client.IDtfClient, wg *sync.WaitGroup) IClean {
	return &cleaner{
		log:    log,
		client: c,
		wg:     wg,
		stop:   make(chan struct{}, 1),
	}
}

func (c *cleaner) Run(ctx context.Context) error {
	c.mutex.Lock()
	if c.stopped {
		defer c.mutex.Unlock()
		return errors.New("dtf cleaner already stop")
	}
	c.wg.Add(1)
	c.mutex.Unlock()

	go func() {
		defer tools.PrintPanicStack()
		defer c.wg.Done()

		c.log.Infof("dtf cleaner run")
		ticker := time.NewTicker(time.Duration(config.CleanInterval) * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-c.stop:
				return
			case <-ticker.C:
				if dbSaga, err := c.client.CleanSAGA(ctx, int64(config.CleanTimeout)*time.Second.Milliseconds()); err != nil {
					c.log.Errorf("dtf cleaner err: %v", err)
					continue
				} else if dbSaga != nil {
					if _, err = go_async.CreateTask(ctx, "", "", dbSaga.Typ, dbSaga.Ctx, true); err != nil {
						c.log.Errorf("sagas %v typ %v compensate start err: %v", dbSaga.UUID, dbSaga.ID, err)
						continue
					}
				}
			}
		}
	}()
	return nil
}

func (c *cleaner) Stop() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// check stop
	if c.stopped {
		c.log.Errorf("dtf cleaner already stop")
		return
	}
	// stop
	c.stopped = true
	c.stop <- struct{}{}
	c.log.Infof("dtf cleaner stop")
}
