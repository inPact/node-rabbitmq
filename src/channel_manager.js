const _ = require('lodash');
const amqp = require('amqplib');
const Promise = require('bluebird');
const debug = require('debug')('tabit:infra:rabbit');

const utils = require('@tabit/utils');
const TopologyBuilder = require('./topology_builder');
const lock = utils.lock;
const Retry = utils.Retry;

let connections = 0;

class RabbitChannelManager {
    /**
     * @param config - the queue configuration to assert.
     * @param [queueName] - the queue to publish to and consume from. If not provided, the @config.name will be used.
     * @param onReconnect
     * @param logger
     */
    constructor(config, onReconnect, { logger = console } = {}) {
        this.config = config;
        this.topologyBuilder = new TopologyBuilder(config);
        this.channels = { pub: null, sub: null };
        this.onReconnect = onReconnect;
        this.logger = logger;

        // if (!config.name &&
        //     !(config.exchange && config.exchange.name))
        //     throw new Error('Invalid queue configuration: a server-named queue must have a name. Queue config: ' +
        //                     JSON.stringify(config, null, 2));
    }

    getPublishChannel() {
        return Promise.resolve(this.channels.pub || this._createChannel('pub'));
    }

    /**
     * @param topic
     * @param [options] {Object}
     * @param [options.name] {String}
     * @param [options.override] {Object} - any desired overrides of the default configuration that was provided
     * when this instance was created.
     * @returns {*}
     */
    getConsumeChannel(topic, options) {
        if (!topic)
            return Promise.resolve(this.channels.sub || this._createChannel('sub'));

        if (options.name)
            return Promise.resolve(this.channels[options.name] || this._createTopicChannel(topic, options));

        return this._createTopicChannel(topic, options);
    }

    getConnection(config = this.config) {
        return lock.getWithDoubleCheck(
            () => this.connection,
            'Queue.getConnection',
            () => this._connect(config),
            connection => this.connection = connection);
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
    _createChannel(channelType, options) {
        if (_.isObject(channelType)) {
            options = channelType;
            channelType = undefined;
        }

        return new Retry(
            () => this.getConnection()
                .then(conn => channelType === 'pub' ? conn.createConfirmChannel() : conn.createChannel())
                .then(channel => {
                    this._manageChannel(channel, channelType);
                    return this.topologyBuilder.assertTopology(channel, options).return(channel);
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
            this.channels[channelType] = channel;
        }

        channel.once('close', () => {
            this.logger.warn('Distributed queue: channel closed');
            this._clearChannel(channel);
        });

        channel.on('error', e => {
            let append = e.stackAtStateChange
                ? '\r\n\tStack at state change: ' + e.stackAtStateChange
                : '';
            this.logger.error('Distributed queue error: ' + utils.errorToString(e) + append);
            this._clearChannel(channel);
        })
    }

    /** @private */
    _connect(config = this.config) {
        let options = config.heartbeat ? '?heartbeat=' + config.heartbeat : '';
        let url = (config.url || 'amqp://localhost') + options;
        debug(`Distributed queue: connecting to ${url}...`);

        return amqp.connect(url)
            .then(connection => {
                this.logger.info(`Distributed queue: connected to ${url}`);
                this.connection = connection;
                debug(`Distributed queue: ${++connections} open connections`);

                connection.on('close', () => {
                    this.logger.warn('Distributed queue: connection closed');
                    debug(`Distributed queue: ${--connections} open connections`);
                    delete this.connection;
                    this.channels = {};

                    if (this.onReconnect)
                        this.onReconnect();
                });

                connection.on('error', e => {
                    this.logger.error('Distributed queue: connection error: ' + utils.errorToString(e));
                });

                return connection;
            });
    }
}

module.exports = RabbitChannelManager;