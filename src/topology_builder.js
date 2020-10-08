const _ = require('lodash');
const Promise = require('bluebird');
const utils = require('@tabit/utils');

class TopologyBuilder {
    constructor(topology) {
        this.topology = topology;
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
    assertTopology(channel, options = {}) {
        let topology = this.topology;

        if (options.override)
            topology = _.merge({}, topology, options.override);

        return Promise.resolve()
            .then(() => {
                if (this.topology.deadLetter)
                    return this.assertDeadLetterExchange(channel, topology.deadLetter);
            })
            .then(() => this.assertExchangeAndQueue(channel, topology, topology.exchange));
    }

    assertExchangeAndQueue(channel, queueConfig = this.topology, exchangeConfig = this.topology.exchange) {
        return utils.promiseIf(
            () => exchangeConfig && exchangeConfig.name,
            () => channel.assertExchange(exchangeConfig.name, exchangeConfig.type))
            .then(() => {
                if (_.get(exchangeConfig, 'type') === 'topic' ||
                    _.get(exchangeConfig, 'bindQueue') === false)
                    return;

                return this.assertQueue(channel, '', queueConfig.name, {}, queueConfig, exchangeConfig);
            })
    }

    /**
     * Asserts a queue named {@link queue} into existence on the {@link channel}. If {@link exchangeConfig} is
     * provided, binds {@link queue} to the exchange named in {@link exchangeConfig} using the provided
     * {@link routingKey}.
     * If {@link exchangeConfig} is provided, this function will also create a private binding between {@link queue}
     * and the exchange named in {@link exchangeConfig} using the queue name (e.g., to be used for requeuing messages
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
    assertQueue(channel, routingKey, queue, options = {}, queueConfig = this.topology, exchangeConfig = this.topology.exchange) {
        if (options.override)
            queueConfig = _.merge({}, queueConfig, options.override);

        _.assign(queueConfig, {
            deadLetterExchange: queueConfig.deadLetter && queueConfig.deadLetter.dlx,
        });

        return channel.assertQueue(queue || '', queueConfig)
            .then(res => {
                channel.__queue = res.queue;

                if (exchangeConfig)
                    return channel.bindQueue(res.queue, exchangeConfig.name, routingKey)
                        // Add a private binding
                        .then(() => channel.bindQueue(res.queue, exchangeConfig.name, res.queue));
            });
    }

     assertDeadLetterExchange(channel, config) {
        return utils.promiseIf(
            () => config.deadLetter,
            () => this.assertDeadLetterExchange(channel, config.deadLetter))
            .then(() => this.assertExchangeAndQueue(channel,
                _.assign({ name: config.dlq }, config),
                { name: config.dlx, type: 'fanout' },
                true));
    }
}

module.exports = TopologyBuilder;
