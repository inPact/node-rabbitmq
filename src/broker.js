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
        this.topologyBuilder = new TopologyBuilder(this.config);
        this.connectionManager = new ConnectionManager(config, { logger: this.logger });
        this.channelManager = new ChannelManager(this.config, {
            logger: this.logger,
            topologyBuilder: this.topologyBuilder,
            connectionManager: this.connectionManager
        });
    }

    createQueue(section, { queueName, sectionOverride } = {}) {
        if (typeof section === 'string')
            section = this.configReader.getQueueConfig(section);

        if (sectionOverride)
            section = _.merge({}, section, sectionOverride);

        return queueFactory.create(section, {
            queueName,
            logger: this.logger,
            channelManager: this.channelManager.forSection(section)
        });
    }

    async getConnection() {
        return await this.connectionManager.getConnection();
    }
};