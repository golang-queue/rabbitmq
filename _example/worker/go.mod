module example_01

go 1.26.10

require (
	github.com/golang-queue/queue v0.6.2
	github.com/golang-queue/rabbitmq v0.2.0
)

require (
	github.com/jpillora/backoff v1.1.0 // indirect
	github.com/rabbitmq/amqp091-go v1.16.0 // indirect
)

replace github.com/golang-queue/rabbitmq => ../../
