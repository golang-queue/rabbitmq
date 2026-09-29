module example_01

go 1.26.8

require (
	github.com/golang-queue/queue v0.5.0
	github.com/golang-queue/rabbitmq v0.1.0
)

require (
	github.com/jpillora/backoff v1.0.0 // indirect
	github.com/rabbitmq/amqp091-go v1.13.0 // indirect
)

replace github.com/golang-queue/rabbitmq => ../../
