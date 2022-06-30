package main

import (
	"encoding/json"
	"flag"
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

var (
	uri          = flag.String("uri", "amqp://guest:guest@localhost:5672/", "AMQP URI")
	exchange     = flag.String("exchange", "test-exchange", "Durable, non-auto-deleted AMQP exchange name")
	exchangeType = flag.String("exchange-type", "direct", "Exchange type - direct|fanout|topic|x-custom")
	q            = flag.String("queue", "test-queue", "Ephemeral AMQP queue name")
	bindingKey   = flag.String("key", "test-key", "AMQP binding key")
)

func init() {
	flag.Parse()
}

func main() {
	taskN := 100

	// define the worker
	w := rabbitmq.NewWorker(
		rabbitmq.WithAddr(*uri),
		rabbitmq.WithQueue(*q),
		rabbitmq.WithExchangeName(*exchange),
		rabbitmq.WithExchangeType(*exchangeType),
		rabbitmq.WithRoutingKey(*bindingKey),
	)

	// define the queue
	q := queue.NewPool(
		0,
		queue.WithWorker(w),
	)

	// assign tasks in queue
	for i := 0; i < taskN; i++ {
		if err := q.Queue(&job{
			Message: fmt.Sprintf("%s: handle the job: %d", *bindingKey, i+1),
		}); err != nil {
			log.Fatal(err)
		}
	}

	time.Sleep(1 * time.Second)
	// shutdown the service and notify all the worker
	q.Release()
}
