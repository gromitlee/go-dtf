package examples

import (
	"context"
	"sync"
	"testing"
	"time"

	go_dtf "github.com/gromitlee/go-dtf"
	"github.com/gromitlee/go-dtf/pkg/sagas"
)

const (
	syncCtx  = "sync"
	asyncCtx = "async"
)

func TestSAGA_Empty(t *testing.T) {
	if err := go_dtf.Init(context.TODO()); err != nil {
		t.Fatal(err)
	}
	saga, _ := go_dtf.NewSAGA(sagaTypeEmpty, testSagasEmpty)
	t.Log(saga.Submit(context.TODO(), testSagasUUID(), syncCtx, sagas.SubmitSync))
	go_dtf.Stop()
}

func TestSAGA_SubmitSync_Success(t *testing.T) {
	if err := go_dtf.Init(context.TODO()); err != nil {
		t.Fatal(err)
	}

	saga, _ := go_dtf.NewSAGA(sagaTypeSuccess, testSagasSuccess)
	if retCtx, err := saga.Submit(context.TODO(), testSagasUUID(), syncCtx, sagas.SubmitSync); err != nil {
		t.Fatal(err)
	} else if retCtx != syncCtx {
		t.Fatal("invalid return")
	}
	go_dtf.Stop()
}

func TestSAGA_SubmitSync_Retry(t *testing.T) {
	if err := go_dtf.Init(context.TODO()); err != nil {
		t.Fatal(err)
	}

	saga, _ := go_dtf.NewSAGA(sagaTypeFail, testSagasFail)
	t.Log(saga.Submit(context.TODO(), testSagasUUID(), syncCtx, sagas.SubmitSync))

	time.Sleep(time.Second * 20)
	go_dtf.Stop()
}

func TestSAGA_SubmitSync_RetryStop(t *testing.T) {
	if err := go_dtf.Init(context.TODO()); err != nil {
		t.Fatal(err)
	}

	saga, _ := go_dtf.NewSAGA(sagaTypeFail, testSagasFail)
	t.Log(saga.Submit(context.TODO(), testSagasUUID(), syncCtx, sagas.SubmitSync))

	time.Sleep(time.Second * 5)
	go_dtf.Stop()
}

func TestSAGA_SubmitSync_Cancel(t *testing.T) {
	if err := go_dtf.Init(context.TODO()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.TODO())
	saga, _ := go_dtf.NewSAGA(sagaTypeFail, testSagasFail)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wg.Done()
		t.Log(saga.Submit(ctx, testSagasUUID(), syncCtx, sagas.SubmitSync))
	}()

	time.Sleep(time.Second * 2)
	cancel()

	time.Sleep(time.Second * 20)
	go_dtf.Stop()
	wg.Wait()
}

func TestSAGA_SubmitAsync_Success(t *testing.T) {
	if err := go_dtf.Init(context.TODO()); err != nil {
		t.Fatal(err)
	}

	saga, _ := go_dtf.NewSAGA(sagaTypeSuccess, testSagasSuccess)
	if _, err := saga.Submit(context.TODO(), testSagasUUID(), asyncCtx, sagas.SubmitAsync); err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second * 5)
	go_dtf.Stop()
}

func TestSAGA_SubmitAsync_Retry(t *testing.T) {
	if err := go_dtf.Init(context.TODO()); err != nil {
		t.Fatal(err)
	}

	saga, _ := go_dtf.NewSAGA(sagaTypeFail, testSagasFail)
	if _, err := saga.Submit(context.TODO(), testSagasUUID(), asyncCtx, sagas.SubmitAsync); err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second * 20)
	go_dtf.Stop()
}

func TestSAGA_SubmitAsync_RetryStop(t *testing.T) {
	if err := go_dtf.Init(context.TODO()); err != nil {
		t.Fatal(err)
	}

	saga, _ := go_dtf.NewSAGA(sagaTypeFail, testSagasFail)
	if _, err := saga.Submit(context.TODO(), testSagasUUID(), asyncCtx, sagas.SubmitAsync); err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second * 5)
	go_dtf.Stop()
}
