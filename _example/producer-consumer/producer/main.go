package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/golang-queue/queue"
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

	// define the worker
	w := rabbitmq.NewWorker(
		rabbitmq.WithSubj("direct_queue"),
		rabbitmq.WithExchangeType(rabbitmq.ExchangeDirect),
		rabbitmq.WithExchangeName("direct_queue"),
	)

	// define the queue
	q := queue.NewPool(
		0,
		queue.WithWorker(w),
	)

	// assign tasks in queue
	for i := 0; i < taskN; i++ {
		if err := q.Queue(&job{
			Message: fmt.Sprintf("handle the job: %d", i+1),
		}); err != nil {
			log.Fatal(err)
		}
	}

	time.Sleep(1 * time.Second)
	// shutdown the service and notify all the worker
	q.Release()
}
