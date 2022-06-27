test:
	go test -v -cover -covermode=atomic -coverprofile=coverage.out

.PHONY: reset
reset:
	rabbitmqctl stop_app
	rabbitmqctl reset
	rabbitmqctl start_app
