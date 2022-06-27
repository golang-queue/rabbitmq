package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

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
	taskN := 100
	rets := make(chan string, taskN)

	// define the worker
	w := rabbitmq.NewWorker(
		rabbitmq.WithSubj("sample_worker"),
		rabbitmq.WithExchangeName("sample_worker"),
		rabbitmq.WithRoutingKey("sample_worker"),
		rabbitmq.WithRunFunc(func(ctx context.Context, m core.QueuedMessage) error {
			var v *job
			if err := json.Unmarshal(m.Bytes(), &v); err != nil {
				return err
			}
			rets <- v.Message
			return nil
		}),
	)

	// define the queue
	q, err := queue.NewQueue(
		queue.WithWorkerCount(5),
		queue.WithWorker(w),
	)
	if err != nil {
		log.Fatal(err)
	}

	// start the five worker
	q.Start()

	// assign tasks in queue
	for i := 0; i < taskN; i++ {
		go func(i int) {
			if err := q.Queue(&job{
				Message: fmt.Sprintf("handle the job: %d", i+1),
			}); err != nil {
				log.Fatal(err)
			}
		}(i)
	}

	// wait until all tasks done
	for i := 0; i < taskN; i++ {
		fmt.Println("message:", <-rets)
		time.Sleep(50 * time.Millisecond)
	}

	// shutdown the service and notify all the worker
	q.Release()
}
