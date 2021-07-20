const { expect } = require('chai');
const { Readable } = require('stream');
const sinon = require('sinon');
const { getFromApi, cleanup, readDataFrom } = require('./common');
const Broker = require('..');
const util = require('util');

const url = 'amqp://localhost';

const DELAY_TOPIC_NAME = 'topic.delay';

describe('send dellayed message: ', function() {

    let broker, queueSection;
    const incomingMessages = new Readable({ objectMode: true, read() { /* Do nothing because no source, we will push */ } });

    before(async function createExchange() {
        broker = new Broker({
            url,
            queues: {
                testDelay: {
                    name: 'test-delayed',
                    exchange: {
                        name: 'topic.delay',
                        type: 'x-delayed-message',
                        arguments: { 'x-delayed-type': 'topic' }
                    },
                }
            }
        });
    });

    before(async function cleanUpIfNeeded() {
        const exchanges = await getFromApi('exchanges');
        const topicDelayExchange = exchanges.find(x => x.name === DELAY_TOPIC_NAME);
        if (topicDelayExchange) await cleanup(broker, DELAY_TOPIC_NAME);
    });

    it('should create delayed messages exchange by consuming (lazy)', async function() {
        queueSection = broker.initQueue('testDelay');
        await queueSection.consume(async (message, { headers }, { routingKey }) => {
            // console.log('Arrived!', typeof message, routingKey, util.inspect(JSON.parse(message)), util.inspect(headers));
            incomingMessages.push({ message, headers, routingKey });
        }, 'retry.*.order');
        const exchanges = await getFromApi('exchanges');
        const topicDelayExchange = exchanges.find(x => x.name === DELAY_TOPIC_NAME);
        expect(topicDelayExchange).to.be.ok;
    });

    it('should publish with delay', async function() {
        await queueSection.publishTo('retry.86.order', 'Message 1', { headers: { 'x-delay': 10000 } });
    });


    it('should receive after delay', function(done) {
        const handleIncomingMessages = sinon.spy(msgPackage => {
            expect(msgPackage).to.have.property('message');
        });
        readDataFrom(incomingMessages, handleIncomingMessages, errors => {
            expect(errors).to.not.be.ok;
            done();
        }, 12000);
    });

    after(async function rabbitCleanup() {
        await cleanup(broker, DELAY_TOPIC_NAME);
    });

});