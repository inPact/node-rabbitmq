const _ = require('lodash');
const Queue = require('./queue_adapter');
const RequestReplyQueue = require('./request_reply_queue');
const DelayedQueue = require('./delayed');

module.exports = {
    create(topologyBuilder, { broker, ...options } = {}) {
        let topology = topologyBuilder.topology;

        if (RequestReplyQueue.isRpc(topology))
            return new RequestReplyQueue(topology, options);

        if (DelayedQueue.isDelayed(topology))
            return new DelayedQueue(topologyBuilder, options);

        return new Queue(topology, options);
    }
};