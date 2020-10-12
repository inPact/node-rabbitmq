const _ = require('lodash');
const Promise = require('bluebird');
const debug = require('debug')('tabit:infra:rabbit');
const uuid = require('uuid');
const EventEmitter = require('events');
const DistributedQueue = require('./distributed_queue');
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
    constructor(section, { queueName, logger = console, channelManager } = {}) {
        super(...arguments);
        this.responseEmitter = new EventEmitter();
        this.responseEmitter.setMaxListeners(0);
    }

    async _listenToReplies() {
        if (!this.listenForResponses) {
            this.listenForResponses = super.consume(
                (data, props) => {
                    debug(`received response on reply-queue "${props.correlationId}", sending to handler...`);
                    this.responseEmitter.emit(props.correlationId, data);
                },
                {
                    noAck: true,
                    name: REPLY_TO_QUEUE,
                    channel: await this.channelManager.getPublishChannel()
                },
            );
        }

        await this.listenForResponses;
        debug(`registered for responses on direct reply-to queue`);
    }

    async consume(handler, topic, options) {
        if (options)
            options = _.omit(options, 'requeueToTail');

        let responder = async (data, props, fields, msg) => {
            debug(`received request on reply-queue "${props.correlationId}", sending to handler...`);
            let response = await handler(data, props, fields);
            debug(`sending response to reply-queue "${props.correlationId}"`);
            await this.publishTo(props.replyTo, this._serialize(response), {
                useBasic: true,
                correlationId: props.correlationId,
                channel: await this.channelManager.getConsumeChannel()
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

        await this._listenToReplies();

        return new Promise(resolve => {
            this.responseEmitter.once(correlationId, async response => resolve(this._deserialize(response)));
            super.publish(entity, options);
        })
    }

    _serialize(response) {
        try {
            if (!response)
                return;
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

