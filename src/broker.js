const _ = require('lodash');
const ChannelManager = require('./channel_manager');
const ConnectionManager = require('./connection_manager');
const TopologyBuilder = require('./topology_builder');
const ConfigReader = require('./config_reader');
const queueFactory = require('./queues');
const BrokerWrapper = require('./broker-wrapper');

module.exports = class Broker {
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

    /**
     *
     * @param section {String|Object}
     * @param [queueName] {String} - override section.name to use the same configuration but for a different queue
     * @param sectionOverride
     * @returns {Queue|Queue}
     */
    createQueue(section, { queueName, sectionOverride = {} } = {}) {
        if (typeof section === 'string')
            section = this.configReader.getQueueConfig(section);

        if (queueName)
            sectionOverride.name = queueName;

        if (sectionOverride)
            section = _.merge({}, section, sectionOverride);

        if (section.limit)
            section.prefetch = section.limit;

        return queueFactory.create(section, {
            queueName,
            logger: this.logger,
            channelManager: this.channelManager.forSection(section)
        });
    }

    async getConnection() {
        return await this.connectionManager.getConnection();
    }

   /**
    * Syntax for the new services
    * @param {string} handle 
    * @param {string} name 
    * @param {object} options 
    * @returns {ConnectionManagerWrapper} the wrapped connection
    */
   static assignNewBroker(handle, name, options) {
       if (!options) options = {};
       if (!options.name) options.name = name || 'Default Connection';
       if (!handle) throw new Error('cannot assign broker without handle');
       if (Broker[handle]) throw new Error(`Cannot assign '${handle}', since already assigned. please pick another name`);
       Broker[handle] = new BrokerWrapper(options);
       return Broker[handle];
   }

};