const _ = require('lodash');
const should = require('chai').should();
const Broker = require('..');
const url = 'amqp://localhost';
const common = require('./common');
const Promise = require('bluebird');

describe('topology should: ', function () {
    let broker;

    afterEach(async function () {
        await common.cleanup(broker);
    });

    it('accept section overrides', async function () {
        broker = new Broker({
            url,
            queues: {
                test: {
                    name: 'test',
                    exchange: { name: 'test' }
                }
            }
        });

        await broker.initQueue('test', {
            queueName: 'custom-name',
            sectionOverride: {
                exchange: {
                    name: 'custom-exchange-name'
                }
            }
        }).consume(x => x);

        let response = await common.getFromApi('exchanges');
        let exchangeNames = response.map(x => x.name);
        exchangeNames.should.include('custom-exchange-name', exchangeNames);
        exchangeNames.should.not.include('test');
    });

    it('build and bind nested dead-letter queues', async function () {
        broker = new Broker({
            url,
            queues: {
                test: {
                    name: 'main',
                    deadLetter: {
                        dlx: 'retry-main-exchange',
                        dlq: 'retry-main',
                        deadLetter: {
                            dlx: 'failed-main-exchange',
                            dlq: 'failed-main',
                        },
                    },
                    exchange: {
                        name: 'main-x',
                        type: 'fanout'
                    }
                },
            }
        });

        await broker.initQueue('test').consume(x => x);

        let exchanges = await common.getFromApi('exchanges');
        let queues = await common.getFromApi('queues');

        let exchangeNames = exchanges.map(x => x.name);
        exchangeNames.should.include('main-x', exchangeNames);
        exchangeNames.should.include('retry-main-exchange', exchangeNames);

        exchangeNames.should.include('failed-main-exchange', exchangeNames);

        let queueNames = queues.map(x => x.name);
        queueNames.should.include('main', exchangeNames);
        queueNames.should.include('retry-main', exchangeNames);
        queueNames.should.include('failed-main', exchangeNames);

        let mainQueue = queues.find(x => x.name === 'main');
        mainQueue.arguments.should.include({ "x-dead-letter-exchange": "retry-main-exchange" });

        let retryQueue = queues.find(x => x.name === 'retry-main');
        retryQueue.arguments.should.include({ "x-dead-letter-exchange": "failed-main-exchange" });
    });

    it('build and bind dead-letter queues with overrides', async function () {
        broker = new Broker({
            url,
            queues: {
                test: {
                    name: 'main',
                    deadLetter: {
                        dlx: 'retry-main-exchange',
                        dlq: 'retry-main',
                        deadLetter: {
                            dlx: 'failed-main-exchange',
                            dlq: 'failed-main',
                        },
                    },
                    exchange: {
                        name: 'main-x',
                        type: 'fanout'
                    }
                },
            }
        });

        await broker.initQueue('test').consume(x => x);
        await broker.initQueue('test', { queueName: 'overriden' }).consume(x => x);

        let exchanges = await common.getFromApi('exchanges');
        let queues = await common.getFromApi('queues');

        let exchangeNames = exchanges.map(x => x.name);
        exchangeNames.should.include('main-x', exchangeNames);
        exchangeNames.should.include('retry-main-exchange', exchangeNames);
        exchangeNames.should.include('failed-main-exchange', exchangeNames);

        let queueNames = queues.map(x => x.name);
        queueNames.should.include('main', exchangeNames);
        queueNames.should.include('overriden', exchangeNames);
        queueNames.should.include('retry-main', exchangeNames);
        queueNames.should.include('failed-main', exchangeNames);

        let mainQueue = queues.find(x => x.name === 'main');
        mainQueue.arguments.should.include({ "x-dead-letter-exchange": "retry-main-exchange" });

        let overridenQueue = queues.find(x => x.name === 'overriden');
        overridenQueue.arguments.should.include({ "x-dead-letter-exchange": "retry-main-exchange" });

        let retryQueue = queues.find(x => x.name === 'retry-main');
        retryQueue.arguments.should.include({ "x-dead-letter-exchange": "failed-main-exchange" });
    });

    it.skip('publish and consume on the same queue-adapter should use separate channels and separate connections', async function () {
        //TODO
    });

    it('use auto-generated queue-name when no queue-name provided', async function () {
        broker = new Broker({
            url,
            queues: {
                test: {
                    exchange: { name: 'test-x' }
                }
            }
        });

        let received = 0;
        let queue = broker.initQueue('test');
        await queue.consume(data => received++);
        await queue.publishTo(null, JSON.stringify({ the: 'entity' }));

        await Promise.delay(100);
        received.should.equal(1);

        let queues = await common.getFromApi('queues');
        let queueNames = queues.map(x => x.name);
        let autoGeneratedQueues = queueNames.filter(x => /^amq\.gen\-.+$/.test(x));
        console.log(`========== autoGeneratedQueues: ${JSON.stringify(autoGeneratedQueues, null, 2)} ==========`);
        autoGeneratedQueues.length.should.be.at.least(1);

    });

    it('not auto-generate queues for publishing', async function () {
        broker = common.createBrokerWithTestQueue();

        let received = 0;
        let queue = broker.initQueue('test');
        await queue.consume(data => received++);
        await broker.initQueue('test').publishTo(null, JSON.stringify({ the: 'entity' }));
        await broker.initQueue('test').publishTo(null, JSON.stringify({ the: 'entity' }));

        await Promise.delay(100);
        received.should.equal(2);

        let queues = await common.getFromApi('queues');
        queues.length.should.equal(1);
    });
});
