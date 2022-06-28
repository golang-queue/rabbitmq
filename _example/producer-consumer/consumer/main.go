package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/appleboy/graceful"
	"github.com/golang-queue/queue"
	"github.com/golang-queue/queue/core"
	"github.com/golang-queue/rabbitmq"
)

type job struct {
	Message string
}

func (j *job) Bytes() []byte {
	b, err := json.Marshal(j)
	if err != nil {
		panic(err)
	}
	return b
}

func main() {
	taskN := 10000
	rets := make(chan string, taskN)

	m := graceful.NewManager()

	// define the worker
	w := rabbitmq.NewWorker(
		rabbitmq.WithQueue("direct_queue"),
		rabbitmq.WithExchangeType(rabbitmq.ExchangeDirect),
		rabbitmq.WithExchangeName("direct_queue"),
		rabbitmq.WithRunFunc(func(ctx context.Context, m core.QueuedMessage) error {
			var v *job
			if err := json.Unmarshal(m.Bytes(), &v); err != nil {
				return err
			}
			rets <- v.Message
			time.Sleep(500 * time.Millisecond)
			return nil
		}),
	)
	// define the queue
	q := queue.NewPool(
		2,
		queue.WithWorker(w),
	)

	m.AddRunningJob(func(ctx context.Context) error {
		for {
			select {
			case <-ctx.Done():
				select {
				case m := <-rets:
					fmt.Println("message:", m)
				default:
				}
				return nil
			case m := <-rets:
				fmt.Println("message:", m)
				time.Sleep(50 * time.Millisecond)
			}
		}
	})

	m.AddShutdownJob(func() error {
		// shutdown the service and notify all the worker
		q.Release()
		return nil
	})

	<-m.Done()
}
