const _ = require('lodash');

class ConfigReader {
    constructor(config) {
        this.amqpConfig = config;
    }

    get amqpParams() {
        return this._amqpParams || (this._amqpParams = _.omit(this.amqpConfig, 'queues'));
    }

    getQueueConfig(queueConfigType) {
        let queueConfig = this.amqpConfig.queues[queueConfigType];
        return _.merge({}, queueConfig, this.amqpParams);
    }
}

module.exports = ConfigReader;