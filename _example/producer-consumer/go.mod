module example

go 1.18

require (
	github.com/appleboy/graceful v0.0.4
	github.com/golang-queue/queue v0.1.3
	github.com/golang-queue/rabbitmq v0.0.3-0.20210907015837-3e2e4b448b3d
)

require (
	github.com/goccy/go-json v0.9.7 // indirect
	github.com/rabbitmq/amqp091-go v1.3.4 // indirect
)

replace github.com/golang-queue/rabbitmq => ../../
