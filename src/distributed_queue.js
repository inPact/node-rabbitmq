const _ = require('lodash');
const Promise = require('bluebird');
const debug = require('debug')('tabit:infra:rabbit');
const verbose = require('debug')('tabit:infra:rabbit:verbose');
const utils = require('@tabit/utils');
const lock = utils.lock;

/**
 * Encapsulates a distributed amqp queue with a single connection
 * and at most one publish channel and one consume channel.
 * @type {Queue}
 */
class Queue {
    /**
     * @param section {Object|String} - the queue configuration to assert, as a full configuration section object or just the
     * name of the section within {@param config} that should be looked up to retrive the configuration section.
     * @param [queueName] - the queue to publish to and consume from. If not provided, the {@param section.name} will be used.
     */
    constructor(section, queueName, { logger = console, channelManager } = {}) {
        if (queueName)
            section.name = queueName;

        this.logger = logger;
        this.consumers = [];
        this.config = section;
        this.queueName = section.name;
        this.exchangeName = _.get(section, 'exchange.name', '');

        this.channelManager = channelManager;
        channelManager.connectionManager.on('closed', this._restartConsumers.bind(this));
    }

    static createCustom(config, section, queueName) {
        let configReader = new ConfigReader(config);
        if (queueName) {
            let realConfig = configReader.getQueueConfig(queueName);
            section = _.merge({}, realConfig, section);
        }

        return new Queue(config, section);
    }

    /**
     *
     * @param entity {Object} - A JSON entity to be serialized and published.
     * @param [options] {Object} - publish options and/or message properties to be published (see amqplib docs).
     * @returns {Promise} - fulfilled once the publish completes.
     */
    publish(entity, options = {}) {
        return this.publishTo(this.queueName, JSON.stringify(entity), options);
    }

    /**
     *
     * @param handler {Function} - a function that is called for each received message, accepting two parameters:
     * 1. the received message
     * 2. additional properties associated with the received messages
     * @param topic
     * @param [options] {Object}
     * @param [options.name] {String}
     * @param [options.limit] {Number} - the prefetch to use vis-a-vis rabbit MQ
     * @param [options.maxRetries] {Number} - the max number of retries if {@param options.requeueToTail} is true.
     * @param [options.requeueToTail] {Boolean} - true to requeue message to tail of queue if {@param handler} fails.
     * After a message is requeued it will be acked. Defaults to false. Messages will be requeued at most
     * {@param options.maxRetries} times, after which they will be nacked.
     * @param [options.override] {Object} - any desired overrides of the default configuration that was provided
     * when this instance was created.
     */
    consume(handler, topic, options) {
        if (_.isObject(topic)) {
            options = topic;
            topic = undefined;
        }

        this.consumers.unshift({ handler, topic, options });

        return this.channelManager.getConsumeChannel(topic, options)
            .then(channel => {
                let consumeOptions = _.merge({}, this.config, options);
                let queue = this.queueName || channel.__queue;

                channel.prefetch(consumeOptions.limit || 100);

                return channel.consume(queue, message => {
                    if (!message)
                        return debug(`consumption cancelled by server`);

                    Promise.resolve(handler(message.content.toString(), message.properties, message.fields))
                        .then(() => {
                            verbose(`Distributed queue: acking 1 to queue "${queue}"`);
                            return channel.ack(message);
                        })
                        .catch(e => {
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
                                        .then(() => debug(`Distributed queue: message requeued to queue "${queue}"`));
                                }
                            }

                            return channel.nack(message, false, false)
                        })
                        .catch(e => this.logger.error('Distributed queue: Consume error handling failed: ' + e.stack))
                })
                    .then(() => {
                        if (this.exchangeName)
                            this.logger.info(`Distributed queue: Consuming messages from exchange "${this.exchangeName}", ` +
                                             `topic "${topic}", queue "${queue}"`);

                        else if (queue)
                            this.logger.info(`Distributed queue: Consuming messages from queue "${queue}"`);

                        if (debug.enabled)
                            debug(`Distributed queue "${queue}" options: `, _.omit(consumeOptions, 'logger'));
                    })
                    .catch(e => this.logger.error('Distributed queue: Consume failed: ' + e.stack));
            });
    }

    _restartConsumers() {
        console.log(`=============================== _restartConsumers ===============================`);
        if (!this.consumers.length)
            return debug(`Distributed queue: Not restarting consumers. No consumers in queue`);

        debug(`Distributed queue: Restarting consumers. Consumers in queue: ${this.consumers.length}`);

        const lockName = 'DistributedQueue._restartConsumers';
        if (lock.internal.isBusy(lockName))
            return debug(`Distributed queue: Consumer restart aborted: lock busy`);

        return lock.acquire(lockName, () => {
            let consumers = _.clone(this.consumers);
            this.consumers.length = 0;

            return utils.promiseWhile(() => consumers.length, () => {
                let consumer = consumers.pop();
                return this.consume(consumer.handler, consumer.topic, consumer.options)
            })
        });
    }
}

/**
 * @param routingKey {string} - the name of the queue to publish to
 * @param message {string} - the message to publish
 * @param [options] - the options to attach to the published message
 * @param [options.persistent] - whether published messages should be persistent or not;
 * defaults to true if not specified.
 * @param done - for internal use
 * @returns {*}
 */
Queue.prototype.publishTo = Promise.promisify(function (routingKey, message, options, done) {
    routingKey = routingKey || this.queueName || '';
    if (arguments.length < 4) {
        done = options;
        options = {};
    }

    if (!options || !_.isBoolean(options.persistent))
        options = _.assign({}, options, { persistent: true });

    this.channelManager.getPublishChannel().then(channel => {
        // TODO: Use confirm-callback instead of received + drain-event?
        let received = channel.publish(this.exchangeName, routingKey, new Buffer(message), options);

        if (received)
            return done();

        this.logger.info(`Distributed queue: publish channel blocking, waiting for drain event.`);
        channel.once('drain', () => {
            this.logger.info(`Distributed queue: drain event received, continuing...`);
            done();
        })
    })
});

module.exports = Queue;