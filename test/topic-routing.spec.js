const _ = require('lodash');
const should = require('chai').should();
const { AssertionError } = require('chai');
const Broker = require('..');
const url = 'amqp://localhost';
const common = require('./common');
const Promise = require('bluebird');

describe('topic routing should: ', function () {
    let broker;

    afterEach(async function () {
        await common.cleanup(broker);
    });

    it('prohibit multiple consumes on different topics on the same queue-adapter with the same queue-name', async function () {
        broker = new Broker({
            url,
            queues: {
                test: {
                    name: 'test',
                    exchange: { name: 'test', type: 'topic' }
                }
            }
        });

        let queueAdapter = broker.initQueue('test');

        // with queue-name override
        try {
            await queueAdapter.consume(x => x, 'routes.one', { name: 'my-queue' });
            await queueAdapter.consume(x => x, 'routes.two', { name: 'my-queue' });
            should.fail('second consume to "my-queue" should fail');
        } catch (e) {
            if (e instanceof AssertionError)
                throw e;

            console.log(e);
        }

        // with default queue-name
        try {
            await queueAdapter.consume(x => x, 'routes.one');
            await queueAdapter.consume(x => x, 'routes.two');
            should.fail('second consume to "test" should fail');
        } catch (e) {
            if (e instanceof AssertionError)
                throw e;

            console.log(e);
        }
    });

    it('allow multiple consumes on different topics on the same queue-adapter with different queue-names', async function () {
        broker = new Broker({
            url,
            queues: {
                test: {
                    name: 'test',
                    exchange: { name: 'test', type: 'topic' }
                }
            }
        });

        let queueAdapter = broker.initQueue('test');

        await queueAdapter.consume(x => x, 'routes.one', { name: 'my-queue' });
        await queueAdapter.consume(x => x, 'routes.two', { name: 'a-different-queue' });
    });

    it('bind consume channel to additional topics', async function () {
        this.timeout(20000);
        await Promise.delay(5000);
        let previousChannelsCount = (await common.getFromApi('channels')).length;
        let previousConsumersCount = (await common.getFromApi('consumers')).length;

        broker = new Broker({
            url,
            queues: {
                test: {
                    name: 'test',
                    exchange: { name: 'test', type: 'topic' }
                }
            }
        });

        let received = 0;
        let queueAdapter = broker.initQueue('test');
        let channel = await queueAdapter.consume(x => received++, 'routes.one', { name: 'my-queue' });
        await channel.addTopics('routes.two'); // check single topic
        await channel.addTopics('routes.three', 'routes.four'); // check multiple topics
        await channel.addTopics('routes.four'); // check duplicate topics

        let publisher = await broker.initQueue('test');
        let theEntity = JSON.stringify({ the: 'entity' });
        await publisher.publishTo('routes.one', theEntity);
        await publisher.publishTo('routes.two', theEntity);
        await publisher.publishTo('routes.three', theEntity);
        await publisher.publishTo('routes.four', theEntity);

        await Promise.delay(500);

        received.should.equal(4);

        console.log(`=============================== waiting for RMQ API to update... ===============================`);
        await Promise.delay(5000);
        let channels = await common.getFromApi('channels');
        let consumers = await common.getFromApi('consumers');

        channels.length.should.equal(previousChannelsCount + 2, 'number of channels: ' + JSON.stringify(channels, null, 2));
        consumers.length.should.equal(previousConsumersCount + 1, 'number of consumers');
    });

    it('prohibit adding topics to non-topic exchanges', async function () {
        broker = new Broker({
            url,
            queues: {
                test: {
                    name: 'test',
                    exchange: { name: 'test', type: 'fanout' }
                }
            }
        });

        let queueAdapter = broker.initQueue('test');
        let channel = await queueAdapter.consume(x => x);
        try {
            await channel.addTopics('routes.two');
            should.fail('adding topics to non-topic exchanges should be prohibited');
        } catch (e) {
            if (e instanceof AssertionError)
                throw e;

            console.log(e);
        }
    });
});
