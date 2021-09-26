const ms = require('ms');
const _ = require('lodash');
const debug = require('debug')('tabit:infra:rabbit:topology');
const FatalError = require('./fatal_error');
const TIME_OPTIONS = ['messageTtl', 'expires'];

class TopologyBuilder {
    /** @private */
    constructor(topology) {
        this.topology = _.cloneDeep(topology);
    }

    static forSection(section) {
        return new TopologyBuilder(section)
    }

    /**
     * Creates the topology matching {@link config}. If a dead letter queue is defined in {@link config}, first creates
     * the dlq.
     * @param channel
     * @param {Object} [options]
     * @param {Object} [options.override] - any desired overrides of the default configuration that was provided
     * when this instance was created.
     * @returns {Promise.<TResult>}
     */
    async assertTopology(channel, options = {}) {
        const topology = this.getOverrideableTopology(options);
        debug(`building topology for channel ${channel.getDescriptor()}: from section: `, this.topology);

        if (this.topology.deadLetter)
            await this.assertDeadLetterExchange(channel, topology.deadLetter);

        await this.assertExchangeAndQueue(channel, topology, topology.exchange);
        debug(`topology for channel ${channel.getDescriptor()} built successfully`);
    }

    async assertExchangeAndQueue(channel, queueConfig = this.topology, exchangeConfig = this.topology.exchange) {
        await this.assertExchange(exchangeConfig, channel);

        if (_.get(exchangeConfig, 'type') === 'topic' ||
            _.get(exchangeConfig, 'bindQueue') === false ||
            channel.__type === 'pub')
            return;

        return await this.assertQueue(channel, '', queueConfig.name, {}, queueConfig, exchangeConfig);
    }

    async assertExchange(exchangeConfig, channel) {
        if (!exchangeConfig && !this.topology.requestReply)
            throw new FatalError(`${channel.getDescriptor()}: building topology failed: an exchange config is required!`)

        if (exchangeConfig && exchangeConfig.name) {
            let exchangeOptions;
            if (exchangeConfig.delayedMessages) {
                exchangeOptions = { arguments: { 'x-delayed-type': exchangeConfig.type } };
                exchangeConfig.type = 'x-delayed-message';
            }

            if (!exchangeConfig.type)
                // although this is the default, making it explicit adds clarity to the topology construction
                exchangeConfig.type = 'direct';

            debug(`asserting exchange "${exchangeConfig.name}" with type "${exchangeConfig.type}" and options:`, exchangeOptions);
            await channel.assertExchange(exchangeConfig.name, exchangeConfig.type, exchangeOptions);
            channel.__exchange = exchangeConfig.name;
        }
    }

    /**
     * Asserts a queue named {@link queue} into existence on the {@link channel}. If {@param exchangeConfig} is
     * provided, binds {@link queue} to the exchange named in {@param exchangeConfig} using the provided
     * {@link routingKey}.
     * If {@param exchangeConfig} is provided, this function will also create a private binding between {@link queue}
     * and the exchange named in {@param exchangeConfig} using the queue name (e.g., to be used for requeuing messages
     * to the tail of a topic-based queue).
     * {@link routingKey}.
     * @param channel
     * @param routingKey
     * @param queueName
     * @param queueConfig
     * @param exchangeConfig
     * @param {Object} [options]
     * @param {Object} [options.override] - any desired overrides of the default configuration that was provided
     * when this instance was created.
     */
    async assertQueue(channel, routingKey, queueName, options = {}, queueConfig = this.topology, exchangeConfig = this.topology.exchange) {
        const topology = this.getOverrideableTopology(options, queueConfig);
        this._setQueueOptions(topology);

        queueName = queueName || topology.name || '';
        debug(`asserting queue "${queueName}" with options: `, _.omit(topology, 'deadLetterExchange', 'url', 'exchange', 'name'));
        let { queue } = await channel.assertQueue(queueName, topology);
        channel.__queue = queue;

        if (exchangeConfig) {
            if (exchangeConfig.useDefault)
                return;

            if (exchangeConfig.type === 'direct')
                routingKey = queueName;

            debug(`binding queue "${queueName}" to exchange "${exchangeConfig.name || '(default)'}" with routing-key "${routingKey}"`);
            await channel.bindQueue(queue, exchangeConfig.name, routingKey);
        }
    }

    _setQueueOptions(topology) {
        _.assign(topology, {
            deadLetterExchange: topology.deadLetter && topology.deadLetter.dlx,
        });

        _.forEach(TIME_OPTIONS, path => {
            let val = _.get(topology, path);
            if (typeof val === 'string')
                _.set(topology, path, ms(val));
        });
    }

    async assertDeadLetterExchange(channel, config) {
        if (config.deadLetter)
            await this.assertDeadLetterExchange(channel, config.deadLetter);

        await this.assertExchangeAndQueue(
            channel,
            _.assign({ name: config.dlq }, config),
            { name: config.dlx, type: 'fanout' },
            true);
    }

    getOverrideableTopology(options = {}, topology = this.topology) {
        if (options.override)
            return _.merge({}, topology, options.override);

        return topology;
    }
}

module.exports = TopologyBuilder;