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
        it('should require exchange declaration', async function () {
            broker = new Broker({
                url: 'amqp://localhost',
                queues: {
                    test: {
                        name: 'test'
                    }
                }
            });

            let queue = broker.initQueue('test');
            await common.assertFails(() => queue.consume(x => x));
        });

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

        it('via multiple direct queues', async function () {
            broker = new Broker({
                url: 'amqp://localhost',
                queues: {
                    testOne: {
                        name: 'test-one',
                        exchange: {
                            name: 'test-x',
                            type: 'direct'
                        }
                    },
                    testTwo: {
                        name: 'test-two',
                        exchange: {
                            name: 'test-x',
                            type: 'direct'
                        }
                    }
                }
            });

            let testOneReceived = 0;
            let testTwoReceived = 0;
            let adapterOne = broker.initQueue('testOne');
            await adapterOne.consume(data => testOneReceived++);
            await adapterOne.publish({ the: 'entity' });

            await Promise.delay(100);
            testOneReceived.should.equal(1);
            testTwoReceived.should.equal(0);

            let adapterTwo = broker.initQueue('testTwo');
            await adapterTwo.consume(data => testTwoReceived++);
            await adapterTwo.publish({ the: 'entity' });

            await Promise.delay(50);
            testOneReceived.should.equal(1);
            testTwoReceived.should.equal(1);

            await adapterOne.publishTo('test-two', JSON.stringify({ the: 'entity' }));

            await Promise.delay(50);
            testOneReceived.should.equal(1);
            testTwoReceived.should.equal(2);
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

    describe('default exchange: ', function () {
        it('should send and receive', async function () {
            broker = new Broker({
                url,
                queues: {
                    test: {
                        name: 'test',
                        exchange: { useDefault: true }
                    }
                }
            });

            let received;
            let queue = broker.initQueue('test');
            await queue.consume(data => received = JSON.parse(data));
            await queue.publish({ the: 'entity' });

            await Promise.delay(50);
            should.exist(received, `message was not received`);
            received.should.deep.equal({ the: 'entity' });
        });

        it('should allow publishing to specified queue', async function () {
            broker = new Broker({
                url,
                queues: {
                    nati: {
                        name: 'nati',
                        exchange: { useDefault: true }
                    },
                    joni: {
                        name: 'joni',
                        exchange: { useDefault: true }
                    }
                }
            });

            let natiReceived = 0;
            let natiQueue = broker.initQueue('nati');
            await natiQueue.consume(data => natiReceived++);
            await natiQueue.publish({ the: 'entity' });

            let joniReceived = 0;
            let joniQueue = broker.initQueue('joni');
            await joniQueue.consume(data => joniReceived++);
            await joniQueue.publish({ the: 'entity' });

            await Promise.delay(50);
            natiReceived.should.equal(1);
        });

        it('should allow publishing to arbitrary queue', async function () {
            broker = new Broker({
                url,
                queues: {
                    defaultExchange: {
                        exchange: { useDefault: true }
                    },
                    test: {
                        name: 'test',
                        exchange: { useDefault: true }
                    }
                }
            });

            let received;
            await broker.initQueue('test').consume(data => received = JSON.parse(data));
            await broker.initQueue('defaultExchange').publishTo('test', JSON.stringify({ the: 'entity' }));

            await Promise.delay(100);
            should.exist(received, `message was not received`);
            received.should.deep.equal({ the: 'entity' });
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

        it('pass all options on to amqplib (e.g. messageTtl)', async function () {
            const MESSAGE_TTL = 100;
            broker = common.createBrokerWithTestQueue({
                rootOptions: {
                    prefetch: 1,
                    messageTtl: MESSAGE_TTL
                }
            });

            let received = 0;
            let queue = broker.initQueue('test');
            await queue.consume(async () => {
                received++;
                await Promise.delay(MESSAGE_TTL * 2);
            });

            await queue.publish({ the: 'entity' });
            await queue.publish({ the: 'entity' });

            await Promise.delay(MESSAGE_TTL * 3);
            received.should.equal(1);
        });
    });
})
;
