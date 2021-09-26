const Queue = require('./queue_adapter');
const RequestReplyQueue = require('./request_reply_queue');

module.exports = {
    create(section, { broker, ...options } = {}) {
        if (section.requestReply)
            return new RequestReplyQueue(section, options);

        return new Queue(section, options);
    }
};