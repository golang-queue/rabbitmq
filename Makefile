
.PHONY: reset
reset:
	rabbitmqctl stop_app
	rabbitmqctl reset
	rabbitmqctl start_app
