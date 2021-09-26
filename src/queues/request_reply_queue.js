const _ = require('lodash');
const Promise = require('bluebird');
const debug = require('debug')('tabit:infra:rabbit:rpc');
const uuid = require('uuid');
const EventEmitter = require('events');
const DistributedQueue = require('./queue_adapter');
const REPLY_TO_QUEUE = 'amq.rabbitmq.reply-to';

/**
 * Encapsulates a rabbitmq direct reply-to queue.
 * @type {Queue}
 */
module.exports = class RequestReplyQueue extends DistributedQueue {
    /**
     * @param section {Object|String} - the queue configuration to assert, as a full configuration section object or just the
     * name of the section within {@param config} that should be looked up to retrive the configuration section.
     * @param [queueName] - the queue to publish to and consume from. If not provided, the {@param section.name} will be used.
     */
    constructor(topology, { queueName, logger = console, channelManager } = {}) {
        super(...arguments);
        this.topology = topology;
        this.channelManager = channelManager;
        this.responseEmitter = new EventEmitter();
        this.responseEmitter.setMaxListeners(0);
    }

    async consume(handler, topic, options) {
        if (options)
            options = _.omit(options, 'requeueToTail');

        let responder = async (data, props, fields, msg) => {
            let channel = await this.channelManager.getConsumeChannel();
            let description = descriptor({ queue: this.topology.name, correlationId: props.correlationId, channel });

            debug(`Q-->* received request from ${description}, sending to handler...`);
            let response = await executeHandler(handler, data, props, fields);

            debug(`Q<--* sending response to ${description}`);
            await this.publishTo(props.replyTo, this._serialize(response), {
                channel,
                correlationId: props.correlationId
            });
        };

        return super.consume(responder, topic, options);
    }

    /**
     * Publish message and handle response when it arrives
     * @param entity - the message to publish
     * @param options
     * @returns {*}
     */
    async publish(entity, options = {}) {
        const correlationId = uuid.v4();
        options.replyTo = REPLY_TO_QUEUE;
        options.correlationId = correlationId;

        let channel = await this.channelManager.getPublishChannel();
        await this._listenToReplies(channel);

        return new Promise(resolve => {
            this.responseEmitter.once(correlationId, async response => resolve(this._deserialize(response)));
            debug(`*-->Q publishing message to ${descriptor({ queue: this.topology.name, correlationId, channel })}`);
            super.publish(entity, { channel, ...options });
        })
    }

    async _listenToReplies(channel) {
        if (!this.listenForResponses)
            this.listenForResponses = this._createReplyListener(channel);

        await this.listenForResponses;
    }

    async _createReplyListener(channel) {
        await super.consume(
            (data, props) => {
                let description = descriptor({ queue: this.topology.name, correlationId: props.correlationId, channel });
                debug(`*<--Q received response from ${description}, sending to handler...`);
                this.responseEmitter.emit(props.correlationId, data);
            },
            {
                channel,
                noAck: true,
                name: REPLY_TO_QUEUE,
            },
        );

        debug(`listening for responses on direct reply-to queue, channel "${channel.getDescriptor()}"`);
    }

    _serialize(response) {
        try {
            if (!response)
                return '';

            return JSON.stringify(response);
        } catch (e) {
            this.logger.error(e);
        }
    }

    _deserialize(response) {
        try {
            if (!response)
                return;

            return JSON.parse(response);
        } catch (e) {
            this.logger.error(e);
        }
    }
};

function descriptor({ queue, correlationId, channel }) {
    let parts = [];
    if (queue)
        parts.push(`queue "${queue}"`);

    if (correlationId)
        parts.push(`with replyTo "${correlationId}"`);

    if (channel)
        parts.push(`on channel "${channel.getDescriptor()}"`);

    return _.join(parts, ' ');
}

async function executeHandler(handler, data, props, fields) {
    try {
        return await handler(data, props, fields);
    } catch (e) {
        return {
            error: {
                message: e.message,
                stack: e.stack
            }
        }
    }
}