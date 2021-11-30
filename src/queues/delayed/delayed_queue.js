const _ = require('lodash');
const QueueAdapter = require('../queue_adapter');
const DelayedPublisher = require('./delayed_publisher');

/**
 * Encapsulates a rabbitmq delayed-messages queue.
 */
module.exports = class DelayedQueue extends QueueAdapter {
    constructor(topologyBuilder, options = {}) {
        let publisher = new DelayedPublisher(topologyBuilder.topology, options);
        super(topologyBuilder.topology, { publisher, ...options });

        configureSection(topologyBuilder.topology);
        topologyBuilder.addDirectQueueBinding()
    }

    static isDelayed(topology) {
        return topology.delayed ||
               // backwards compatibility
               _.get(topology, 'exchange.delayedMessages');
    }
};

function configureSection(topology) {
    if (!topology.exchange)
        topology.exchange = {};

    _.merge(topology.exchange, { arguments: { 'x-delayed-type': topology.exchange.type || 'direct' } });
    topology.exchange.type = 'x-delayed-message';
}