const { expect } = require('chai');
const { Readable } = require('stream');
const sinon = require('sinon');
const { getFromApi, cleanup, readDataFrom } = require('./common');
const Broker = require('..');
const util = require('util');

const url = 'amqp://localhost';

const DELAY_TOPIC_NAME = 'topic.delay';

describe('send dellayed message: ', function() {

    this.timeout(0);

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
        await queueSection.publishTo('retry.86.order', 'Message 1', { headers: { 'x-delay': 12000 } });
    });


    it('should not receive message during delay', function(done) {
        const handleIncomingMessages = sinon.stub();
        readDataFrom(incomingMessages, handleIncomingMessages, errors => {
            expect(handleIncomingMessages.callCount).to.equal(0);
            done();
        }, 10000);
    });

    it('should receive message after delay', function(done) {
        const handleIncomingMessages = sinon.stub();
        readDataFrom(incomingMessages, handleIncomingMessages, errors => {
            expect(handleIncomingMessages.callCount).to.equal(1);
            const firstCallArg = handleIncomingMessages.firstCall.args[0];
            expect(firstCallArg).to.have.property('message');
            expect(firstCallArg.message).to.equal('Message 1');
            expect(firstCallArg).to.have.property('headers');
            done();
        }, 4000);
    });

    after(async function rabbitCleanup() {
        await cleanup(broker, DELAY_TOPIC_NAME);
    });

});