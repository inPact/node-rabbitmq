const _ = require('lodash');
const Promise = require('bluebird');
const debug = require('debug')('tabit:infra:rabbit:topology');

class TopologyBuilder {
    constructor(topology) {
        this.topology = _.omit(topology, 'logger');
    }

    /**
     * Creates the topology matching {@link config}. If a dead letter queue is defined in {@link config}, first creates
     * the dlq.
     * @param channel
     * @param [options] {Object}
     * @param [options.override] {Object} - any desired overrides of the default configuration that was provided
     * when this instance was created.
     * @returns {Promise.<TResult>}
     */
    async assertTopology(channel, options = {}) {
        debug(`building topology: `, this.topology);
        const topology = this.getOverrideableTopology(options);

        if (this.topology.deadLetter)
            await this.assertDeadLetterExchange(channel, topology.deadLetter);

        await this.assertExchangeAndQueue(channel, topology, topology.exchange);
        debug(`topology built successfully`);
    }

    async assertExchangeAndQueue(channel, queueConfig = this.topology, exchangeConfig = this.topology.exchange) {
        if (exchangeConfig && exchangeConfig.name)
            await channel.assertExchange(exchangeConfig.name, exchangeConfig.type);

        if (_.get(exchangeConfig, 'type') === 'topic' ||
            _.get(exchangeConfig, 'bindQueue') === false)
            return;

        return await this.assertQueue(channel, '', queueConfig.name, {}, queueConfig, exchangeConfig);
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
     * @param queue
     * @param queueConfig
     * @param exchangeConfig
     * @param [options] {Object}
     * @param [options.override] {Object} - any desired overrides of the default configuration that was provided
     * when this instance was created.
     */
    async assertQueue(channel, routingKey, queue, options = {}, queueConfig = this.topology, exchangeConfig = this.topology.exchange) {
        const topology = this.getOverrideableTopology(options, queueConfig);

        // Where there is no override, there will be a mutation here (I'm not sure why) -- Nati
        _.assign(topology, {
            deadLetterExchange: topology.deadLetter && topology.deadLetter.dlx,
        });

        let res = await channel.assertQueue(queue || topology.name || '', topology);
        channel.__queue = res.queue;

        if (exchangeConfig) {
            await channel.bindQueue(res.queue, exchangeConfig.name, routingKey);
            // Add a private binding
            await channel.bindQueue(res.queue, exchangeConfig.name, res.queue);
        }
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
