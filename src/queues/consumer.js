const _ = require('lodash');
const debug = require('debug')('tabit:infra:rabbit');
const verbose = require('debug')('tabit:infra:rabbit:verbose');
const utils = require('@tabit/utils');

const DEFAULT_ACK_TIMEOUT_MS = 45 * 60 * 1000;

/**
 * Encapsulates a distributed amqp queue with a single connection
 * and at most one publish channel and one consume channel.
 * @type {Queue}
 */
class Consumer {
    /**
     * @param {Object} topology
     * @param {Object|String} section The queue configuration to assert, as a full configuration section object or just the name of the section within.
     * @param {Object} [logger] Logger to log
     * @param {ChannelManager} [channelManager] - the associated channel manager
     */
    constructor(topology, { logger = console, channelManager, publisher } = {}) {
        this.publisher = publisher;
        this.logger = logger;
        this.consumers = [];
        this.topology = topology;
        this.exchange = _.get(topology, 'exchange', {});
        this.exchangeName = this.exchange.name || '';
        this.handleTimeout = DEFAULT_ACK_TIMEOUT_MS;

        this.channelManager = channelManager;
        channelManager.connectionManager.on('closed', this._restartConsumers.bind(this));
    }

    setHandleTimeout(timeoutMs) {
        if (!timeoutMs || !_.isNumber(timeoutMs)) throw new Error(`Bad handler timeout: ${timeoutMs}`);
        this.handleTimeout = timeoutMs;
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

        let consumeOptions = _.merge({}, this.topology, options);

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

                let timeoutRef;

                try {

                    // Run handler with timeout:
                    await Promise.race([
                        handler(message.content.toString(), message.properties, message.fields, message).finally(() => {
                            clearTimeout(timeoutRef);
                        }),
                        new Promise(r => timeoutRef = setTimeout(r, this.handleTimeout)).then(() => {
                            const error = new Error(`Tabit-Rabbit timeout of ${this.handleTimeout}ms for handler to finish, is over`);
                            error.isTimeOutError = true;
                            return Promise.reject(error);
                        }),
                    ]);

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

                        // Note from Nati:
                        // To be backwards compatible, if normal requeue is configured is always takes precedence,
                        // even if error is a timeout:
                        const requeueToTail = (
                            consumeOptions.requeueToTail ||
                            (e.isTimeOutError && consumeOptions.onTimeoutRequeueToTail)
                        );
                        const maxRetries = (
                            consumeOptions.maxRetries ||
                            (e.isTimeOutError && consumeOptions.onTimeoutMaxRetries)
                        );

                        if (requeueToTail) {
                            if (!maxRetries || deliveryAttempts < maxRetries) {
                                message.properties.headers = _.omit(message.properties.headers, 'x-death');
                                let properties = _.merge(message.properties, {
                                    headers: { 'x-delivery-attempts': ++deliveryAttempts }
                                });

                                return this.publisher.publishTo(queue, message.content.toString(), { basic: true, ...properties })
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
                throw new Error(`A consumer is already configured for queue "${queue}". If you meant to add bindings, use the "channel.addTopics" method instead`);
        }
    }
}

module.exports = Consumer;