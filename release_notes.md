## NEXT
* [TAB-12547] Support of delayed messages using RabbitMQ extension

## 3.0.0
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