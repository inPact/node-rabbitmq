const Promise = require('bluebird');
const should = require('chai').should();
const Broker = require('..');
const url = 'amqp://localhost';
const common = require('./common');

describe('messaging: ', function () {
    let broker;

    afterEach(async function () {
        await common.cleanup(broker);
    });

    describe('should send and receive: ', function () {
        it('via direct queue', async function () {
            broker = common.createBrokerWithTestQueue({ name: 'test-basic' });

            let received;
            let queue = broker.initQueue('test');
            await queue.consume(data => received = JSON.parse(data));
            await queue.publish({ the: 'entity' });

            await Promise.delay(100);
            should.exist(received, `message was not received`);
            received.should.deep.equal({ the: 'entity' });
        });

        it('via basic queue', async function () {
            broker = new Broker({
                url,
                queues: {
                    test: { name: 'test' }
                }
            });

            let received;
            let queue = broker.initQueue('test');
            await queue.consume(data => received = JSON.parse(data));
            await queue.publishTo(null, JSON.stringify({ the: 'entity' }), { useBasic: true });

            await Promise.delay(100);
            should.exist(received, `message was not received`);
            received.should.deep.equal({ the: 'entity' });
        });

        it('via fanout queue', async function () {
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
    });

    describe('should reconnect: ', function () {
        it('consumers and publishers after disconnect', async function () {
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

        it('consumers from multiple brokers after disconnect', async function () {
            let broker1 = common.createBrokerWithTestQueue({ name: 'test1' });
            let broker2 = common.createBrokerWithTestQueue({ name: 'test2' });

            let received1 = 0;
            let received2 = 0;

            await broker1.initQueue('test').consume(x => received1++);
            await broker2.initQueue('test').consume(x => received2++);

            await broker1.initQueue('test').publish({ the: 'entity' });
            await broker2.initQueue('test').publish({ the: 'entity' });

            await Promise.delay(100);
            received1.should.equal(1);
            received2.should.equal(1);

            console.log(`=============================== closing connections and waiting for recovery... ===============================`);
            let broker1Connection = await broker1.getConnection();
            let broker2Connection = await broker2.getConnection();

            // close connections simultaneously
            broker1Connection.close();
            broker2Connection.close();
            await Promise.delay(500);
            console.log(`=============================== done waiting. re-testing... ===============================`);

            await broker1.initQueue('test').publish({ the: 'entity' });
            await broker2.initQueue('test').publish({ the: 'entity' });

            await Promise.delay(100);
            received1.should.equal(2);
            received2.should.equal(2);
        });
    });

    describe('with options should: ', function () {
        it('modify prefetch options from queue section', async function () {
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

            // wait for the remaining messages to clear out
            await Promise.delay(1000);
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
});
