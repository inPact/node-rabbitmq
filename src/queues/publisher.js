const ms = require('ms');
const _ = require('lodash');
const utility = require('../utility');
const Promise = require('bluebird');
const debug = require('debug')('tabit:infra:rabbit');
const TIME_OPTIONS = ['expiration', 'delay'];

/**
 * Encapsulates a distributed amqp queue with a single connection
 * and at most one publish channel and one consume channel.
 */
module.exports = class Publisher {
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
     * @param channel
     * @param {Boolean} [basic] - true to override the default behavior and publish the message via the default
     * amqp exchange rather than the exchange configured for this publisher. When publishing via the default exchange,
     * the routing key denotes the queue that the message will be routed to.
     * Essentially, if true, the publish will behave like amqplib#sendToQueue (see {@link http://www.squaremobius.net/amqp.node/channel_api.html#channel_sendToQueue}).
     * @param {Object} [options={}] the options to attach to the published message.
     * @param {Object} [options.channel] override default amqplib channel.
     * @param {boolean} [options.persistent] whether published messages should be persistent or not
     * defaults to true if not specified.
     * @param {boolean} [options.done] for internal use.
     * @returns a Promise that resolves when the publish completes.
     */
    async publishTo(routingKey = '', message, { channel, basic, ...options } = {}) {
        return new Promise(async (resolve, reject) => {
            try {
                options = this._setAndVerifyOptions(options);

                if (!channel)
                    channel = await this.channelManager.getPublishChannel();

                let exchangeName = basic ? '' : this.exchangeName;
                routingKey = this._getRoutingKey(routingKey, channel);

                debug(`publishing message to exchange "${exchangeName}" with routing-key "${routingKey}"`);
                // TODO: Use confirm-callback instead of received + drain-event?

                let received = channel.publish(exchangeName, routingKey, Buffer.from(message), options);
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

    _setAndVerifyOptions(options) {
        if (!options || !_.isBoolean(options.persistent))
            options = _.assign({}, options, { persistent: true });

        return this._setAllMilliseconds(options);
    }

    _setAllMilliseconds(options){
        return utility.setAllMilliseconds(options, TIME_OPTIONS);
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