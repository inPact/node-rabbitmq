## NEXT
* added explicit method to add/remove topic-bindings to/from existing queues via methods on consume-channel
* fixed bug that that was preventing all consumers from restarting after disconnection when using multiple brokers
* fixed bug where consuming auto-generated queues would explode
* fixed bug (strange design decision) in which queues were always given an extra binding from the exchange directly to the queue, 
  which probably served the pub/sub via direct strategy, despite this not being the recommended way of doing it 
* fixed bug where queues would be asserted by publishers and even generate multiple queues when using auto-generated
  queues (by not defining a queue-name)
* improved topology traces  
* working with default exchange with direct routing to specific queues made clearer and more explicit
  BREAKING CHANGES: For clarity, defining a non-rpc section without an explicit exchange configuration is now prohibited 
* BREAKING CHANGES: direct channels are no longer bound without routing key (thus they no longer behave like fanout queues)  
* internally, removed amqplib#sendToQueue usage in favor of publishing with empty exchange-name
  BREAKING CHANGES: "useBasic" flag in #publish and #publishTo is no longer supported

## 3.2.0
* Return error response if RPC handler throws an error (instead of never responding)
* revert @tabit/utils version to ros-compatible version

## 3.1.0
* Support of delayed messages using RabbitMQ extension

# 3.0.0
* BREAKING CHANGES: 
  * change "createQueue" to "initQueue"
  * use queue-name from topology-configuration if no queue-name was provided in the options to "assertQueue" method  

## 2.1.1
* more readable traces for request-reply debugging
* fix bug in which parallel request-reply publishes on the same queue wrapper would cause 
  unnecessary publish channels to be opened

## 2.1.0
* use more standard name for prefetch configuration
* override default configuration with queue configuration

### 2.0.1
* various bug fixes

# 2.0.0
* added request-reply support using rabbitmq direct-reply-to queues

## 1.2.0
* breaking change: change createQueue signature to make queueName optional

## 1.1.0
* support overriding section configuration when defining queue

# 1.0.0
* ready for initial testing

## 0.1.0
* ported from ROS