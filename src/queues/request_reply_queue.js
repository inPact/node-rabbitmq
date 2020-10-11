const _ = require('lodash');
const Promise = require('bluebird');
const debug = require('debug')('tabit:infra:rabbit');
const verbose = require('debug')('tabit:infra:rabbit:verbose');
const uuid = require('uuid');
const EventEmitter = require('events');
const DistributedQueue = require('../distributed_queue');
const REPLY_TO_QUEUE = 'amq.rabbitmq.reply-to';

let registeredForReplies;

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
        if (!registeredForReplies) {
            await super.consume(
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

            registeredForReplies = true;
            debug(`registered for responses on direct reply-to queue`);
        }

        return registeredForReplies;
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
        }

        return super.consume(responder, topic, options);
    }

    /**
     * Publish message and handle response when it arrives
     * @param entity - the message to publish
     * @param handler {Function} - a function to handle the response, that receives a single argument: the response.
     * @param publishOptions
     * @returns {*}
     */
    async publishAndWait(entity, handler, publishOptions = {}) {
        const correlationId = uuid.v4();
        publishOptions.replyTo = REPLY_TO_QUEUE;
        publishOptions.correlationId = correlationId;

        await this._listenToReplies();

        return new Promise((resolve, reject) => {
            this.responseEmitter.once(correlationId, async response => {
                try {
                    let result = await handler(response);
                    resolve(result);
                } catch (e) {
                    reject(e);
                }
            });
            this.publish(entity, publishOptions);
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
}

