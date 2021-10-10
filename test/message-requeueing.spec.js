const Promise = require('bluebird');
const should = require('chai').should();
const Broker = require('..');
const url = 'amqp://localhost';
const common = require('./common');

describe('requeueToTail', function () {
    let broker;

    afterEach(async function () {
        await common.cleanup(broker);
    });

    describe('on direct exchanges: ', function () {
        it('should ack and requeue until success', async function () {
            const MANUAL_REQUEUE_COUNT_STOP_CONDITION = 10;
            broker = new Broker({
                url,
                queues: {
                    test: {
                        name: 'test',
                        exchange: { name: 'test' },
                        requeueToTail: true
                    }
                }
            });


            let received = [];
            let queue = broker.initQueue('test');

            await queue.consume(async (message, { headers }) => {
                received.push({ message })

                if (headers['x-delivery-attempts'] >= MANUAL_REQUEUE_COUNT_STOP_CONDITION)
                    return;

                throw new Error('requeue');
            });

            await queue.publish({ the: 'entity' });

            await Promise.delay(300);
            received.length.should.equal(MANUAL_REQUEUE_COUNT_STOP_CONDITION);
        });

        it('should ack and requeue until configured max retries reached', async function () {
            const MAX_RETRIES = 5;
            broker = new Broker({
                url,
                queues: {
                    test: {
                        name: 'test',
                        exchange: { name: 'test' },
                        requeueToTail: true,
                        maxRetries: MAX_RETRIES
                    }
                }
            });

            let received = [];
            let queue = broker.initQueue('test');

            await queue.consume(async (message, { headers }) => {
                received.push({ message })
                throw new Error('requeue');
            });

            await queue.publish({ the: 'entity' });

            await Promise.delay(300);
            received.length.should.equal(MAX_RETRIES);
        });
    });

    describe('on fanout exchanges: ', function () {
        it('should ack and requeue only to the queue that failed', async function () {
            const MAX_RETRIES = 5;
            broker = new Broker({
                url,
                queues: {
                    test: {
                        name: 'test',
                        exchange: {
                            name: 'test',
                            type: 'fanout'
                        },
                        requeueToTail: true,
                        maxRetries: MAX_RETRIES
                    }
                }
            });

            let queueOneReceived = [];
            let queueTwoReceived = [];

            await broker.initQueue('test', { queueName: 'q1' }).consume(message => {
                queueOneReceived.push(message);
                throw new Error('requeue');
            });
            await broker.initQueue('test', { queueName: 'q2' }).consume(message => {
                queueTwoReceived.push(message)
            });

            await broker.initQueue('test').publish({ the: 'entity' });

            await Promise.delay(300);
            queueOneReceived.length.should.equal(MAX_RETRIES);
            queueTwoReceived.length.should.equal(1);
        });
    });

    describe('on topic exchanges: ', function () {
        it('should ack and requeue only to the queue that failed', async function () {
            const MAX_RETRIES = 5;
            broker = new Broker({
                url,
                queues: {
                    test: {
                        name: 'test',
                        exchange: {
                            name: 'test',
                            type: 'topic'
                        },
                        requeueToTail: true,
                        maxRetries: MAX_RETRIES
                    }
                }
            });

            let queueOneReceived = [];
            let queueTwoReceived = [];

            let queueAdapter = broker.initQueue('test');
            await queueAdapter.consume(message => {
                queueOneReceived.push(message);
                throw new Error('requeue');
            }, 'routes.one', { name: 'q1' });

            await queueAdapter.consume(message => queueTwoReceived.push(message), 'routes.#', { name: 'q2' });
            await queueAdapter.publishTo('routes.one', 'the entity');

            await Promise.delay(300);
            queueOneReceived.length.should.equal(MAX_RETRIES);
            queueTwoReceived.length.should.equal(1);
        });
    });
});
