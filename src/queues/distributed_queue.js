const _ = require('lodash');
const Promise = require('bluebird');
const debug = require('debug')('tabit:infra:rabbit');
const verbose = require('debug')('tabit:infra:rabbit:verbose');
const utils = require('@tabit/utils');

/**
 * Encapsulates a distributed amqp queue with a single connection
 * and at most one publish channel and one consume channel.
 * @type {Queue}
 */

/** Class representing a queue section. */
class Queue {

    /**
     * Create queue section
     * @param {Object|String} section The queue configuration to assert, as a full configuration section object or just the name of the section within.
     * @param {Object} [options] Optional
     * @param {Object} [options.logger] Logger to log
     * @param {ChannelManager} [options.channelManager] - the associated channel manager
     */
    constructor(section, { logger = console, channelManager } = {}) {
        this.logger = logger;
        this.consumers = [];
        this.config = section;
        this.exchangeName = _.get(section, 'exchange.name', '');

        this.channelManager = channelManager;
        channelManager.connectionManager.on('closed', this._restartConsumers.bind(this));
    }

    /**
     *
     * @param {Function} handler - a function that is called for each received message, accepting two parameters:
     * 1. the received message
     * 2. additional properties associated with the received messages
     * @param {String} [topic]
     * @param channel - override default amqplib channel
     * @param {Object} [options]
     * @param {String} [options.name]
     * @param {Number} [options.limit] - the prefetch to use vis-a-vis rabbit MQ
     * @param {Number} [options.maxRetries] - the max number of retries if {@param options.requeueToTail} is true.
     * @param {Boolean} [options.requeueToTail] - true to requeue message to tail of queue if {@param handler} fails.
     * After a message is requeued it will be acked. Defaults to false. Messages will be requeued at most
     * {@param options.maxRetries} times, after which they will be nacked.
     * @param {Object} [options.override] - any desired overrides of the default configuration that was provided
     * when this instance was created.
     * @return {amqplib.channel}
     */
    async consume(handler, topic, { channel, ...options } = {}) {
        if (_.isObject(topic)) {
            ({ channel, ...options } = topic);
            topic = undefined;
        }

        let consumeOptions = _.merge({}, this.config, options);

        if (!channel) {
            await this._validateConsumeChannel(consumeOptions.name);
            channel = await this.channelManager.getConsumeChannel(topic, options);
        }

        let queue = consumeOptions.name || channel.__queue;
        this.consumers.unshift({ channel, handler, topic, options, queue });

        channel.prefetch(consumeOptions.prefetch || 100);

        try {
            await channel.consume(queue, async message => {
                if (!message)
                    return debug(`consumption cancelled by server`);

                debug(`received message on queue "${queue}", sending to handler...`);

                try {
                    await handler(message.content.toString(), message.properties, message.fields, message);
                    if (!options.noAck) {
                        verbose(`acking 1 to queue "${queue}"`);
                        return channel.ack(message);
                    }
                } catch (e) {
                    try {
                        let deliveryAttempts = message.properties.headers && message.properties.headers['x-delivery-attempts'] || 1;
                        let append = '';

                        if (!e.handled)
                            append = ` with error ${utils.errorToString(e)}`;

                        this.logger.warn(`Distributed queue: delivery attempt #${deliveryAttempts} to queue "${queue}" ` +
                                         `failed${append}`);

                        if (consumeOptions.requeueToTail) {
                            if (!consumeOptions.maxRetries || deliveryAttempts < consumeOptions.maxRetries) {
                                message.properties.headers = _.omit(message.properties.headers, 'x-death');
                                let properties = _.merge(message.properties, {
                                    headers: { 'x-delivery-attempts': ++deliveryAttempts }
                                });

                                return this.publishTo(queue, message.content.toString(), properties)
                                    .then(() => channel.ack(message))
                                    .then(() => debug(`message requeued to queue "${queue}"`));
                            }
                        }

                        return channel.nack(message, false, false)
                    } catch (e) {
                        this.logger.error('Distributed queue: Consume error handling failed: ' + e.stack)
                    }
                }
            }, consumeOptions);

            if (this.exchangeName)
                this.logger.info(`Distributed queue: Consuming messages from exchange "${this.exchangeName}", ` +
                                 `topic "${topic}", queue "${queue}"`);

            else if (queue)
                this.logger.info(`Distributed queue: Consuming messages from queue "${queue}"`);

            if (debug.enabled)
                debug(`Consuming messages from channel "${channel.getDescriptor()}"${topic ? ` on topic "${topic}"` : ''} with options: `, _.omit(consumeOptions, 'logger'));

            return channel;
        } catch (e) {
            this.logger.error('Distributed queue: Consume failed: ' + e.stack);
        }
    }

    /**
     *
     * @param {Object} entity - A JSON entity to be serialized and published.
     * @param {Object} [options] - publish options and/or message properties to be published (see amqplib docs).
     * @returns {Promise} - fulfilled once the publish completes.
     */
    publish(entity, options = {}) {
        return this.publishTo('', JSON.stringify(entity), options);
    }

    /**
     * @param {string} routingKey the name of the queue or topic to publish to.
     * @param {string} message the message to publish.
     * @param {Object} [options={}] the options to attach to the published message.
     * @param {Object} [options.channel] override default amqplib channel.
     * @param {boolean} [options.useBasic]
     * @param {boolean} [options.persistent] whether published messages should be persistent or not
     * defaults to true if not specified.
     * @param {boolean} [options.done] for internal use.
     * @returns {PromiseLike<any>}
     */
    async publishTo(routingKey = '', message, { channel, useBasic, ...options } = {}) {
        return new Promise(async (resolve, reject) => {
            try {
                if (!options || !_.isBoolean(options.persistent))
                    options = _.assign({}, options, { persistent: true });

                if (options.delay) {
                    if (!_.isNumber(options.delay)) throw new Error('options.delay must be a number');
                    if (options.delay < 0) throw new Error('options.delay is negative, cannot travel to the past');
                    if (!_.get(this.config, 'exchange.delayedMessages')) {
                        throw new Error('to publish a delayed message please configure the exchange with delayedMessage');
                    }
                    options = _.merge({}, _.omit(options, 'delay'), { headers: { 'x-delay': options.delay } });
                }

                if (!channel)
                    channel = await this.channelManager.getPublishChannel();

                routingKey = this._getRoutingKey(routingKey, channel, useBasic);

                debug(`publishing message to route or queue "${routingKey}"`);
                // TODO: Use confirm-callback instead of received + drain-event?
                let received = useBasic
                    ? channel.sendToQueue(routingKey, Buffer.from(message), options)
                    : channel.publish(this.exchangeName, routingKey, Buffer.from(message), options);

                if (received)
                    return resolve();

                this.logger.info(`Distributed queue: publish channel blocking, waiting for drain event.`);
                channel.once('drain', () => {
                    this.logger.info(`Distributed queue: drain event received, continuing...`);
                    resolve();
                })
            } catch (e) {
                reject(e);
            }
        })
    }

    _getRoutingKey(routingKey, channel, useBasic) {
        if (routingKey)
            return routingKey;

        if (this.config.requestReply)
            return this.config.name;

        if (useBasic)
            return channel.__queue;

        return '';
    }

    _restartConsumers() {
        if (!this.consumers.length)
            return debug(`Not restarting consumers. No consumers in queue`);

        debug(`Restarting consumers. Consumers in queue: ${this.consumers.length}`);

        let consumers = _.clone(this.consumers);
        this.consumers.length = 0;

        return utils.promiseWhile(() => consumers.length, () => {
            let consumer = consumers.pop();
            return this.consume(consumer.handler, consumer.topic, consumer.options)
        })
    }

    _validateConsumeChannel(queue) {
        if (this.consumers.length) {
            let sameQueueConsumer = this.consumers.find(x => x.queue === queue);
            if (sameQueueConsumer)
                throw new Error('Multiple consumers registered to the same queue. If you meant to add bindings, use the "channel.addTopics" method instead');
        }
    }
}

module.exports = Queue;