const Promise = require('bluebird');
const should = require('chai').should();
const Broker = require('..');
const url = 'amqp://localhost';
const common = require('./common');

describe('send and receive should: ', function () {
    it('send and receive via direct queue', async function () {
        let broker = new Broker({
            url,
            queues: {
                testBasic: {
                    name: 'test-basic',
                    exchange: { name: 'test-basic' }
                }
            }
        });
        try {
            let received;
            let queue = broker.initQueue('testBasic');
            await queue.consume(data => received = JSON.parse(data));
            await queue.publish({ the: 'entity' });

            await Promise.delay(100);
            should.exist(received, `message was not received`);
            received.should.deep.equal({ the: 'entity' });
        } finally {
            await common.cleanup(broker, 'test-basic', 'test-basic');
        }
    });

    it('send and receive via basic queue', async function () {
        let broker = new Broker({
            url,
            queues: {
                testBasic: { name: 'test-basic' }
            }
        });
        try {
            let received;
            let queue = broker.initQueue('testBasic');
            await queue.consume(data => received = JSON.parse(data));
            await queue.publishTo(null, JSON.stringify({ the: 'entity' }), { useBasic: true });

            await Promise.delay(100);
            should.exist(received, `message was not received`);
            received.should.deep.equal({ the: 'entity' });
        } finally {
            await common.cleanup(broker, 'test-basic', 'test-basic');
        }
    });

    it('send and receive via fanout queue', async function () {
        let broker = new Broker({
            url,
            queues: {
                testBasic: {
                    name: 'test-basic',
                    exchange: {
                        name: 'test-basic',
                        type: 'fanout'
                    }
                }
            }
        });
        try {
            let received = [];
            await broker.initQueue('testBasic', { queueName: 'q1' }).consume(data => {
                received.push(JSON.parse(data))
            });
            await broker.initQueue('testBasic', { queueName: 'q2' }).consume(data => {
                received.push(JSON.parse(data))
            });
            await broker.initQueue('testBasic').publish({ the: 'entity' });

            await Promise.delay(100);
            received.length.should.equal(2);
        } finally {
            await common.cleanup(broker, 'test-basic', 'q1', 'q2');
        }
    });

    it('reconnect consumers and publishers after disconnect', async function () {
        let broker = new Broker({
            url,
            queues: {
                test: {
                    name: 'test',
                    exchange: {
                        name: 'test',
                        type: 'fanout'
                    }
                }
            }
        });
        try {
            let received = [];
            await broker.initQueue('test', 'q1').consume(data => received.push(JSON.parse(data)));
            let pubQueue = broker.initQueue('test');
            await pubQueue.publish({ the: 'entity' });

            await Promise.delay(100); // wait for pub/sub
            received.length.should.equal(1);

            (await broker.getConnection()).close();
            await Promise.delay(500); // wait for close and reconnect
            await pubQueue.publish({ the: 'entity' });
            await Promise.delay(100); // wait for pub/sub
            received.length.should.equal(2);
        } finally {
            await common.cleanup(broker, 'test', 'test');
        }
    });

    it('merge options from root config', async function () {
        const PREFETCH = 5;
        let broker = new Broker({
            url,
            prefetch: PREFETCH,
            queues: {
                test: {
                    name: 'test',
                    exchange: { name: 'test' }
                }
            }
        });
        try {
            let handling = 0;
            let queue = broker.initQueue('test');
            await queue.consume(async x => {
                handling++;
                await Promise.delay(PREFETCH * 30);
            });

            for (let i of new Array(PREFETCH * 2).fill(1)) {
                await queue.publish({ the: 'entity' });
            }

            await Promise.delay(PREFETCH * 10);
            handling.should.equal(PREFETCH);
        } finally {
            await common.cleanup(broker, 'test', 'test');
        }
    });

    it('override root config options with queue options', async function () {
        const PREFETCH = 4;
        let broker = new Broker({
            url,
            prefetch: PREFETCH * 2,
            queues: {
                test: {
                    prefetch: PREFETCH,
                    name: 'test',
                    exchange: { name: 'test' }
                }
            }
        });
        try {
            let handling = 0;
            let queue = broker.initQueue('test');
            await queue.consume(async x => {
                handling++;
                await Promise.delay(PREFETCH * 30);
            });

            for (let i of new Array(PREFETCH * 2).fill(1)) {
                await queue.publish({ the: 'entity' });
            }

            await Promise.delay(PREFETCH * 10);
            handling.should.equal(PREFETCH);
        } finally {
            await common.cleanup(broker, 'test', 'test');
        }
    });
});
