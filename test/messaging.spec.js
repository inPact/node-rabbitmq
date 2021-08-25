const Promise = require('bluebird');
const should = require('chai').should();
const Broker = require('..');
const url = 'amqp://localhost';
const common = require('./common');

describe('send and receive should: ', function () {
    let broker;

    afterEach(async function () {
        await common.cleanup(broker);
    });

    it('send and receive via direct queue', async function () {
        broker = common.createBrokerWithTestQueue({ exchangeType: 'fanout', name: 'test-basic' });

        let received;
        let queue = broker.initQueue('test');
        await queue.consume(data => received = JSON.parse(data));
        await queue.publish({ the: 'entity' });

        await Promise.delay(100);
        should.exist(received, `message was not received`);
        received.should.deep.equal({ the: 'entity' });
    });

    it('send and receive via basic queue', async function () {
        broker = new Broker({
            url,
            queues: {
                testBasic: { name: 'test-basic' }
            }
        });

        let received;
        let queue = broker.initQueue('testBasic');
        await queue.consume(data => received = JSON.parse(data));
        await queue.publishTo(null, JSON.stringify({ the: 'entity' }), { useBasic: true });

        await Promise.delay(100);
        should.exist(received, `message was not received`);
        received.should.deep.equal({ the: 'entity' });
    });

    it('send and receive via fanout queue', async function () {
        broker = common.createBrokerWithTestQueue({ exchangeType: 'fanout', name: 'test-basic' });

        let received = [];
        await broker.initQueue('test', { queueName: 'q1' }).consume(data => {
            received.push(JSON.parse(data))
        });
        await broker.initQueue('test', { queueName: 'q2' }).consume(data => {
            received.push(JSON.parse(data))
        });
        await broker.initQueue('test').publish({ the: 'entity' });

        await Promise.delay(100);
        received.length.should.equal(2);
    });

    it('reconnect consumers and publishers after disconnect', async function () {
        broker = common.createBrokerWithTestQueue({ exchangeType: 'fanout' });

        let received = [];
        await broker.initQueue('test').consume(data => received.push(JSON.parse(data)));
        let publisher = broker.initQueue('test');
        await publisher.publish({ the: 'entity' });

        await Promise.delay(100); // wait for pub/sub
        received.length.should.equal(1);

        (await broker.getConnection()).close();
        await Promise.delay(500); // wait for close and reconnect
        await publisher.publish({ the: 'entity' });
        await Promise.delay(100); // wait for pub/sub
        received.length.should.equal(2);
    });

    it('merge options from root config', async function () {
        const PREFETCH = 5;
        broker = new Broker({
            url,
            prefetch: PREFETCH,
            queues: {
                test: {
                    name: 'test',
                    exchange: { name: 'test' }
                }
            }
        });

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
    });

    it('override root config options with queue options', async function () {
        const PREFETCH = 4;
        broker = new Broker({
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
    });
});
