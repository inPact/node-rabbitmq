const _ = require('lodash');
const Promise = require('bluebird');
const debug = require('debug')('tabit:infra:rabbit');

/**
 * Encapsulates a distributed amqp queue with a single connection
 * and at most one publish channel and one consume channel.
 * @type {Queue}
 */
class Publisher {
    /**
     * @param {Object} topology
     * @param {Object|String} section The queue configuration to assert, as a full configuration section object or just the name of the section within.
     * @param {Object} [options.logger] Logger to log
     * @param {ChannelManager} [options.channelManager] - the associated channel manager
     */
    constructor(topology, { logger = console, channelManager } = {}) {
        this.logger = logger;
        this.topology = topology;
        this.exchange = _.get(topology, 'exchange', {});
        this.exchangeName = this.exchange.name || '';
        this.useDefaultExchange = this.exchange.useDefault;

        this.channelManager = channelManager;
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
        return new Promise(async (resolve, reject) => {
            try {
                if (!options || !_.isBoolean(options.persistent))
                    options = _.assign({}, options, { persistent: true });

                if (options.delay)
                    options = this._getAndVerifyDelayOptions(options);

                if (!channel)
                    channel = await this.channelManager.getPublishChannel();

                routingKey = this._getRoutingKey(routingKey, channel);

                debug(`publishing message to route or queue "${routingKey}"`);
                // TODO: Use confirm-callback instead of received + drain-event?

                let received = channel.publish(this.exchangeName, routingKey, Buffer.from(message), options);
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

    _getAndVerifyDelayOptions(options) {
        if (!_.isNumber(options.delay))
            throw new Error('options.delay must be a number');

        if (options.delay < 0)
            throw new Error('options.delay is negative, cannot travel to the past');

        if (!_.get(this.topology, 'exchange.delayedMessages'))
            throw new Error('to publish a delayed message please configure the exchange with delayedMessage');

        return _.merge({}, _.omit(options, 'delay'), { headers: { 'x-delay': options.delay } });
    }

    _getRoutingKey(routingKey, channel) {
        if (routingKey)
            return routingKey;

        if (this.topology.requestReply)
            return this.topology.name;

        if (this.useDefaultExchange || this.exchange.type === 'direct')
            return channel.__queue || this.topology.name;

        return '';
    }
}

module.exports = Publisher;