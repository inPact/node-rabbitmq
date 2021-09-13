# opinionated amqplib wrapper for Rabbit MQ

Why opinionated? Unless told otherwise, will:
* always set a default prefetch of 100
* always publish messages as persistent (and leave the amqplib default of setting all queues as durable)
* consumer-friendly request-reply-queue implementation
* and more...

**Running tests:**

  Either use the available docker-compose to run on a container, OR

  Install dependencies locally:
  * Erlang
  * RabbitMQ
  * enable the management plugin
  * download the delayed-messages plugin: https://github.com/rabbitmq/rabbitmq-delayed-message-exchange/releases
  * enable the delayed-messages plugin: https://github.com/rabbitmq/rabbitmq-delayed-message-exchange
  
Then run _mocha ./test_