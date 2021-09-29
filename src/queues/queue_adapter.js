const Publisher = require('./publisher');
const Consumer = require('./consumer');

/**
 * Encapsulates a distributed amqp queue with a single connection
 * and at most one publish channel and one consume channel.
 * @type {QueueAdapter}
 */
class QueueAdapter {
    /**
     * Create a pub/sub adapter
     * @param {Object} topology
     * @param {Object|String} section - The queue configuration to assert, as a full configuration section object or just the name of the section within.
     * @param {Object} [logger] - Logger to log
     * @param {ChannelManager} [channelManager] - the associated channel manager
     */
    constructor(topology, { logger = console, channelManager } = {}) {
        this.publisher = new Publisher(...arguments);
        this.consumer = new Consumer(topology, { logger, channelManager, publisher: this.publisher });
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
        return this.consumer.consume(...arguments);
    }

    /**
     *
     * @param {Object} entity - A JSON entity to be serialized and published.
     * @param {Object} [options] - publish options and/or message properties to be published (see amqplib docs).
     * @returns {Promise} - fulfilled once the publish completes.
     */
    publish(entity, options = {}) {
        return this.publisher.publishTo('', JSON.stringify(entity), options);
    }

    /**
     * @param {string} routingKey the name of the queue or topic to publish to.
     * @param {string} message the message to publish.
     * @param {Object} [options={}] the options to attach to the published message.
     * @param {Object} [options.channel] override default amqplib channel.
     * @param {boolean} [options.persistent] whether published messages should be persistent or not
     * defaults to true if not specified.
     * @param {boolean} [options.done] for internal use.
     * @returns a Promise that resolves when the publish completes.
     */
    async publishTo(routingKey = '', message, { channel, ...options } = {}) {
        return this.publisher.publishTo(...arguments);
    }
}

module.exports = QueueAdapter;