# opinionated amqplib wrapper for Rabbit MQ

Why opinionated? Unless told otherwise, will:
* always set a default prefetch of 100
* always publish messages as persistent
* consumer-friendly request-reply-queue implementation
* and more...

**Running tests:**

  Either use the available docker-compose to run on a container, OR

  Install dependencies locally:
  * Erlang
  * RabbitMQ
  * enable the management plugin
  * download the delayed-messages plugin: https://github.com/rabbitmq/rabbitmq-delayed-message-exchange/releases
  * copy the plugin (.ez file) to your plugins folder (example path to folder on windows: C:\Program Files\RabbitMQ Server\rabbitmq_server-3.8.19\plugins)
  * enable the delayed-messages plugin: https://github.com/rabbitmq/rabbitmq-delayed-message-exchange
  
Then run _mocha ./test_
