# opinionated amqplib wrapper for Rabbit MQ

Why opinionated? Unless told otherwise, will:
* always set a default prefetch of 100
* always publish messages as persistent (and leave the amqplib default of setting all queues as durable)
* consumer-friendly request-reply-queue implementation
* and more...

**Tracing:**
* To enable all traces, set the `DEBUG` environment variable to `tabit:infra:rabbit:*`
* Additional more specific traces are also available (e.g., `tabit:infra:rabbit:topology`, etc)

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
