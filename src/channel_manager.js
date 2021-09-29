const _ = require('lodash');
const utils = require('@tabit/utils');
const Retry = utils.Retry;

class ChannelManager {
    /**
     * Create channel manager
     * @param {ConnectionManager} connectionManager The associated connection manager
     * @param {TopologyBuilder} topologyBuilder The associated topology builder
     * @param {Object} [options] Optional options
     * @param {Object} [options.logger] Logger to log
     */
    constructor(connectionManager, topologyBuilder, { logger = console } = {}) {
        this.topologyBuilder = topologyBuilder;
        this.topology = topologyBuilder.topology;
        this.connectionManager = connectionManager;
        this.channels = { pub: null, sub: null };
        this.logger = logger;
    }

    async getPublishChannel() {
        if (!this.channels.pub)
            this.channels.pub = this._createChannel('pub');

        return this.channels.pub;
    }

    /**
     * @param topic
     * @param {Object} [options]
     * @param {String} [options.name]
     * @param {Object} [options.override] - any desired overrides of the default configuration that was provided
     * when this instance was created.
     * @returns amqplib channel
     */
    async getConsumeChannel(topic, options) {
        if (!topic) {
            if (!this.channels.sub)
                this.channels.sub = this._createChannel('sub');

            return this.channels.sub;
        }

        return this._createTopicChannel(topic, options);
    }

    getConnection(config) {
        return this.connectionManager.getConnection(config);
    }

    /**
     *
     * @param topic
     * @param {Object} [options]
     * @param {String} [options.name]
     * @param {Object} [options.override] - any desired overrides of the default configuration that was provided
     * when this instance was created.
     * @returns amqplib channel
     * @private
     */
    _createTopicChannel(topic, options = {}) {
        return this._createChannel(options)
            .then(channel => this.topologyBuilder.assertQueue(channel, topic, options.name, options)
                .then(() => {
                    if (options.name) {
                        channel.__name = options.name;
                        this.channels[options.name] = channel;
                    }

                    return channel;
                }))
            .catch(e => {
                this.logger.error(`Rabbit channel manager error: ${e.stack}`);
                throw e;
            })
    }

    /**
     *
     * @param {"pub"|"sub"} [channelType] - indicates whether this channel should be created as publish channel
     * (with auto-confirm) or a subscribe channel (without auto-confirm)
     * @param {Object} [options]
     * @param {Object}[options.override] - any desired overrides of the default configuration that was provided
     * when this instance was created.
     * @returns amqplib channel
     * @private
     */
    async _createChannel(channelType, options) {
        if (_.isObject(channelType)) {
            options = channelType;
            channelType = 'sub';
        }

        return await new Retry(
            () => this.getConnection()
                .then(conn => channelType === 'pub' ? conn.createConfirmChannel() : conn.createChannel())
                .then(async channel => {
                    this._manageChannel(channel, channelType);
                    await this.topologyBuilder.assertTopology(channel, options);
                    return channel;
                }),
            {
                delay: 1000,
                maxTime: Infinity,
                title: 'Distributed queue',
                retryErrorMatch: e => e.retry !== false
            })
            .execute();
    }

    /** @private */
    _clearChannel(channel) {
        if (channel.__type || channel.__name)
            delete this.channels[channel.__type || channel.__name];
    }

    /** @private */
    _manageChannel(channel, channelType) {
        channel.getDescriptor = function () {
            return descriptor(this);
        };

        let topology = this.topologyBuilder.topology;
        channel.addTopics = async function (...patterns) {
            verifyTopicExchange(this, topology);

            for (let pattern of patterns)
                await this.bindQueue(this.__queue, this.__exchange, pattern);
        };

        channel.removeTopics = async function (...patterns) {
            verifyTopicExchange(this, topology);

            for (let pattern of patterns)
                await this.unbindQueue(this.__queue, this.__exchange, pattern);
        };

        if (channelType) {
            channel.__type = channelType;
            this.channels[channelType] = channel;
        }

        channel.once('close', () => {
            this.logger.warn(`Distributed queue: channel "${channel.getDescriptor()}" closed`);
            this._clearChannel(channel);
        });

        channel.on('error', e => {
            let append = e.stackAtStateChange
                ? '\r\n\tStack at state change: ' + e.stackAtStateChange
                : '';
            this.logger.error(`Distributed queue error in channel "${channel.getDescriptor()}": ` + utils.errorToString(e) + append);
            this._clearChannel(channel);
        });
    }
}

function verifyTopicExchange(channel, topology) {
    if (!channel.__exchange || !topology.exchange || topology.exchange.type !== 'topic')
        throw new Error('cannot add topics to non-topic exchanges');
}

function descriptor(channel) {
    let isSub = channel.__type === 'sub';
    let parts = ['x:' + (channel.__exchange || '(default)')];

    if (isSub && (channel.__name || channel.__queue))
        parts.push('q:' + (channel.__name || channel.__queue));

    if (channel.__type)
        parts.push(channel.__type);

    if (!isSub)
        _.reverse(parts);

    return `${_.join(parts, '->')}(${channel.ch})`;
}

module.exports = ChannelManager;