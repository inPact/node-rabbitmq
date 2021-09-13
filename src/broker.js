const _ = require('lodash');
const ChannelManager = require('./channel_manager');
const ConnectionManager = require('./connection_manager');
const TopologyBuilder = require('./topology_builder');
const ConfigReader = require('./config_reader');
const queueFactory = require('./queues');

module.exports = class {
    constructor(config) {
        this.logger = config.logger || console;
        this.config = config;
        this.configReader = new ConfigReader(this.config);
        this.connectionManager = new ConnectionManager(config, { logger: this.logger });
    }

    /**
     * Init section a.k.a queue and it's related exchanges, dead letters etc.
     * Assertion will be made lazy, a.k.a on consume / publish.
     * @param section {String|Object} if section is a string, it's a ref to a named section provided at the Broker construction
     * @param {Object} [options={}] Additional options
     * @param {string} [options.queueName] override section.name to use the same configuration but for a different queue
     * @param {Object} [options.sectionOverride] overrides various things at section
     * @returns {Queue} the section
     */
    initQueue(section, { queueName, sectionOverride = {} } = {}) {
        if (typeof section === 'string')
            section = this.configReader.getQueueConfig(section);

        section = fixSection(section, queueName, sectionOverride);
        let topologyBuilder = TopologyBuilder.forSection(section);

        return queueFactory.create(topologyBuilder.topology, {
            queueName,
            logger: this.logger,
            channelManager: new ChannelManager(this.connectionManager, topologyBuilder, { logger: this.logger })
        });
    }

    async getConnection() {
        return await this.connectionManager.getConnection();
    }

    disconnect() {
        return this.connectionManager.dispose();
    }
};

// TODO: move this to topology builder? Is all of this needed?
function fixSection(section, queueName, sectionOverride) {
    if (queueName)
        sectionOverride.name = queueName;

    if (sectionOverride)
        section = _.merge({}, section, sectionOverride);

    if (section.limit)
        section.prefetch = section.limit;

    return section;
}