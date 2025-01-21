module example

go 1.22

require (
	github.com/appleboy/graceful v0.0.4
	github.com/golang-queue/queue v0.3.0
	github.com/golang-queue/rabbitmq v0.0.3-0.20210907015837-3e2e4b448b3d
)

require (
	github.com/jpillora/backoff v1.0.0 // indirect
	github.com/rabbitmq/amqp091-go v1.10.0 // indirect
)

replace github.com/golang-queue/rabbitmq => ../../
