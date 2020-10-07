const Queue = require('./distributed_queue');
const ChannelManager = require('./channel_manager');

module.exports = class {
    constructor(config){
        this.config = config;
    }

    createQueue(section, queueName){
        return new Queue(this.config, section, queueName);
    }

    async getConnection(){
        return await new ChannelManager(this.config).getConnection();
    }
};