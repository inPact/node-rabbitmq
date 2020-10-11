const Queue = require('../distributed_queue');
const RequestReplyQueue = require('./request_reply_queue');

module.exports = {
    create(section, { broker, ...options } = {}) {
        if (section.requestReply)
            return new RequestReplyQueue(section, options)

        return new Queue(section, options);
    }
}