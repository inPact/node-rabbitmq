const _ = require('lodash');
const utils = require('@tabit/utils');

const TopologyBuilder = require('./topology_builder');
const ConnectionManager = require('./connection_manager');
const Retry = utils.Retry;

class ChannelManager {
    /**
     * @param config - the queue configuration to assert.
     * @param [queueName] - the queue to publish to and consume from. If not provided, the @config.name will be used.
     * @param logger
     */
    constructor(config, { logger = console, topologyBuilder, connectionManager } = {}) {
        this.config = config;
        this.connectionManager = connectionManager || new ConnectionManager(config, { logger });
        this.topologyBuilder = topologyBuilder || new TopologyBuilder(config);
        this.channels = { pub: null, sub: null };
        this.logger = logger;
    }

    /**
     *
     * @param section
     * @returns {ChannelManager}
     */
    forSection(section) {
        return new ChannelManager(section, {
            logger: this.logger,
            connectionManager: this.connectionManager,
            topologyBuilder: new TopologyBuilder(section)
        });
    }

    async getPublishChannel() {
        return await (this.channels.pub || this._createChannel('pub'));
    }

    /**
     * @param topic
     * @param [options] {Object}
     * @param [options.name] {String}
     * @param [options.override] {Object} - any desired overrides of the default configuration that was provided
     * when this instance was created.
     * @returns {*}
     */
    async getConsumeChannel(topic, options) {
        if (!topic)
            return await (this.channels.sub || this._createChannel('sub'));

        return this._createTopicChannel(topic, options);
    }

    getConnection(config = this.config) {
        return this.connectionManager.getConnection(config);
    }

    /**
     *
     * @param topic
     * @param [options] {Object}
     * @param [options.name] {String}
     * @param [options.override] {Object} - any desired overrides of the default configuration that was provided
     * when this instance was created.
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
     * @param [channelType] {String}
     * @param [options] {Object}
     * @param [options.override] {Object} - any desired overrides of the default configuration that was provided
     * when this instance was created.
     * @returns {*}
     * @private
     */
    async _createChannel(channelType, options) {
        if (_.isObject(channelType)) {
            options = channelType;
            channelType = undefined;
        }

        return await new Retry(
            () => this.getConnection()
                .then(conn => channelType === 'pub' ? conn.createConfirmChannel() : conn.createChannel())
                .then(async channel => {
                    this._manageChannel(channel, channelType);
                    await this.topologyBuilder.assertTopology(channel, options);
                    return channel;
                }),
            { delay: 1000, maxTime: Infinity, title: 'Distributed queue' })
            .execute();
    }

    /** @private */
    _clearChannel(channel) {
        if (channel.__type || channel.__name)
            delete this.channels[channel.__type || channel.__name];
    }

    /** @private */
    _manageChannel(channel, channelType) {
        if (channelType) {
            channel.__type = channelType;
            channel.getDescriptor = function () {
                return descriptor(this);
            };
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
        })
    }
}

function descriptor(channel) {
    let parts = [];
    if (channel.__name || channel.__queue)
        parts.push(channel.__name || channel.__queue);

    if (channel.__type)
        parts.push(channel.__type);

    return `${_.join(parts, ':')}(${channel.ch})`;
}

module.exports = ChannelManager;