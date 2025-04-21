package examples

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/gromitlee/go-dtf/pkg/sagas"
)

const (
	sagaTypeEmpty   uint32 = 999
	sagaTypeSuccess uint32 = 111
	sagaTypeFail    uint32 = 222
)

func testSagasUUID() string {
	return "sagas_" + strconv.Itoa(int(time.Now().UnixMicro()))
}

func testSagasEmpty() sagas.ISAGAS {
	return &testSagas{
		steps: []sagas.IStepTransaction{
			&testSagasStep{},
			&testSagasStep{},
			&testSagasStep{},
		},
	}
}

func testSagasSuccess() sagas.ISAGAS {
	return &testSagas{
		steps: []sagas.IStepTransaction{
			&testSagasStep{
				tf: subTrans("trans_t_0", 0, 0),
				cf: subTrans("trans_c_0", 5, 5),
			},
			&testSagasStep{
				tf: subTrans("trans_t_1", 0, 0),
				cf: subTrans("trans_c_1", 5, 5),
			},
			&testSagasStep{
				tf: subTrans("trans_t_2", 0, 0),
				cf: subTrans("trans_c_3", 5, 5),
			},
		},
	}
}

func testSagasFail() sagas.ISAGAS {
	return &testSagas{
		steps: []sagas.IStepTransaction{
			&testSagasStep{
				tf: subTrans("trans_t_0", 0, 0),
				cf: subTrans("trans_c_0", 5, 5),
			},
			&testSagasStep{
				tf: subTrans("trans_t_1", 0, 0),
				cf: subTrans("trans_c_1", 5, 5),
			},
			&testSagasStep{
				tf: subTrans("trans_t_2", 10, 10),
				cf: subTrans("trans_c_2", 5, 5),
			},
		},
	}
}

type testSagas struct {
	steps []sagas.IStepTransaction
}

func (s *testSagas) Transactions() []sagas.IStepTransaction {
	return s.steps
}

type testSagasStep struct {
	tf sagas.Func
	cf sagas.Func
}

func (s *testSagasStep) OnTransact() sagas.Func {
	return s.tf
}

func (s *testSagasStep) OnCompensate() sagas.Func {
	return s.cf
}

func subTrans(name string, failRate, panicRate int) func(context.Context, string) (string, error) {
	return func(ctx context.Context, sagaCtx string) (string, error) {
		log.Printf("=== %v start ...", name)

		rn := rand.New(rand.NewSource(time.Now().UnixNano()))
		timer := time.NewTimer(time.Second)
		ticker := time.NewTicker(time.Millisecond * 100)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return "", fmt.Errorf("%v canceled", name)
			case <-ticker.C:
				if rn.Intn(100) < panicRate {
					panic(fmt.Sprintf("%v panic", name))
				}
				if rn.Intn(100) < failRate {
					return "", fmt.Errorf("%v error", name)
				}
			case <-timer.C:
				log.Printf("=== %v finish ===", name)
				return sagaCtx, nil
			}
		}
	}
}
