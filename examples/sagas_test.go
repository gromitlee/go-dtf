package examples

import (
	"context"
	"sync"
	"testing"
	"time"

	go_async "github.com/gromitlee/go-async"

	go_dtf "github.com/gromitlee/go-dtf"
	"github.com/gromitlee/go-dtf/pkg/sagas"
)

func TestSAGAS_None(t *testing.T) {
	if err := dtfInit(context.TODO()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 30)
	dtfStop()
}

func TestSAGAS_Empty(t *testing.T) {
	if err := dtfInit(context.TODO()); err != nil {
		t.Fatal(err)
	}
	if retCtx, err := go_dtf.SubmitSAGAS(context.TODO(), sagaTypeEmpty, testSagasUUID(), syncCtx, sagas.SubmitSync); err != nil {
		t.Fatal(err)
	} else if retCtx != syncCtx {
		t.Fatal("invalid return")
	}
	dtfStop()
}

func TestSAGAS_SubmitSync_Success(t *testing.T) {
	if err := dtfInit(context.TODO()); err != nil {
		t.Fatal(err)
	}
	if retCtx, err := go_dtf.SubmitSAGAS(context.TODO(), sagaTypeSuccess, testSagasUUID(), syncCtx, sagas.SubmitSync); err != nil {
		t.Fatal(err)
	} else if retCtx != syncCtx {
		t.Fatal("invalid return")
	}
	dtfStop()
}

func TestSAGAS_SubmitSync_RetryStopAndRestart(t *testing.T) {
	if err := dtfInit(context.TODO()); err != nil {
		t.Fatal(err)
	}

	t.Log(go_dtf.SubmitSAGAS(context.TODO(), sagaTypeFail, testSagasUUID(), syncCtx, sagas.SubmitSync))

	time.Sleep(time.Second * 10)
	dtfStop()

	// restart
	if err := dtfInit(context.TODO()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 10)
	dtfStop()
}

func TestSAGAS_SubmitSync_CancelStopAndRestart(t *testing.T) {
	if err := dtfInit(context.TODO()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.TODO())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wg.Done()
		t.Log(go_dtf.SubmitSAGAS(ctx, sagaTypeFail, testSagasUUID(), syncCtx, sagas.SubmitSync))
	}()

	time.Sleep(time.Second * 2)
	cancel()
	wg.Wait()

	time.Sleep(time.Second * 10)
	dtfStop()

	// restart
	if err := dtfInit(context.TODO()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 20)
	dtfStop()
}

func TestSAGAS_SubmitAsync_Success(t *testing.T) {
	if err := dtfInit(context.TODO()); err != nil {
		t.Fatal(err)
	}

	if _, err := go_dtf.SubmitSAGAS(context.TODO(), sagaTypeSuccess, testSagasUUID(), asyncCtx, sagas.SubmitAsync); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 5)
	dtfStop()
}

func TestSAGAS_SubmitAsync_RetryStopAndRestart(t *testing.T) {
	if err := dtfInit(context.TODO()); err != nil {
		t.Fatal(err)
	}

	if _, err := go_dtf.SubmitSAGAS(context.TODO(), sagaTypeFail, testSagasUUID(), asyncCtx, sagas.SubmitAsync); err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second * 10)
	dtfStop()

	// restart
	if err := dtfInit(context.TODO()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 20)
	dtfStop()
}

func dtfInit(ctx context.Context) error {
	db := getDB(dbMysql, dbName)

	if err := go_async.Init(ctx, db); err != nil {
		return err
	}
	if err := go_dtf.Init(ctx, go_dtf.WithDB(db)); err != nil {
		return err
	}
	if err := go_dtf.RegisterSAGAS(sagaTypeEmpty, testSagasEmpty); err != nil {
		return err
	}
	if err := go_dtf.RegisterSAGAS(sagaTypeSuccess, testSagasSuccess); err != nil {
		return err
	}
	if err := go_dtf.RegisterSAGAS(sagaTypeFail, testSagasFail); err != nil {
		return err
	}
	return nil
}

func dtfStop() {
	go_dtf.Stop()
	go_async.Stop()
}
