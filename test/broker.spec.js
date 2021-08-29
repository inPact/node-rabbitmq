const _ = require('lodash');
const should = require('chai').should();
const Broker = require('..');
const url = 'amqp://localhost';
const common = require('./common');
const Promise = require('bluebird');

describe('broker should: ', function () {
    let broker;

    afterEach(async function () {
        await common.cleanup(broker);
    });

    it('close all connections without consumer recovery', async function () {
        broker = new Broker({
            url,
            queues: {
                test: {
                    name: 'test',
                    exchange: { name: 'test' }
                }
            }
        });

        await broker.initQueue('test').consume(x => x);
        await broker.initQueue('testBasic').publish({ the: 'entity' });

        await broker.disconnect();

        console.log(`=============================== waiting for RMQ API to update... ===============================`);
        await Promise.delay(5000);

        let connections = await common.getFromApi('connections');
        let channels = await common.getFromApi('channels');
        let consumers = await common.getFromApi('consumers');

        connections.length.should.equal(0, 'number of connections');
        channels.length.should.equal(0, 'number of channels');
        consumers.length.should.equal(0, 'number of consumers');
    });
});
