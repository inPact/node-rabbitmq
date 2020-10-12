# opinionated amqplib wrapper for Rabbit MQ

Why opinionated? Unless told otherwise, will:
* always set a default prefetch of 100
* always publish messages as persistent
* consumer-friendly request-reply-queue implementation
* and more...
