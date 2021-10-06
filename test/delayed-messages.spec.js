const { expect } = require('chai');
const { Readable } = require('stream');
const Promise = require('bluebird');
const sinon = require('sinon');
const common = require('./common');
const Broker = require('..');

const url = 'amqp://localhost';

const DELAY_EXCHANGE_NAME = 'topic.delay';
const QUEUE_NAME = 'test-delayed';

function createDelayableBroker({ queueOptions } = {}) {
    return new Broker({
        url,
        queues: {
            testDelay: {
                name: QUEUE_NAME,
                exchange: {
                    name: DELAY_EXCHANGE_NAME,
                    type: 'topic',
                    delayedMessages: true,
                },
                ...queueOptions
            }
        }
    });
}

let broker;

async function cleanup() {
    await common.cleanup(broker);
}

describe('Delayed messages', function () {
    const incomingMessages = new Readable({
        objectMode: true, read() { /* Do nothing because no source, we will push */
        }
    });

    before('stopping logging', async function () {
        sinon.stub(console, 'log');
        sinon.stub(console, 'info');
    });

    describe('special exchange for delayed messages', function () {

        before('cleanup', cleanup);

        let exchanges = [];

        before('creating', async function () {
            broker = createDelayableBroker();
            const queueAdapter = broker.initQueue('testDelay');
            await queueAdapter.consume(() => {
            }, 'blabla-topic'); // Without any consume or publish it won't get created..
        });

        before('get the exchanges', async function () {
            exchanges = await common.getFromApi('exchanges');
        });

        it('should get created (lazy)', async function () {
            const topicDelayExchange = exchanges.find(x => x.name === DELAY_EXCHANGE_NAME);
            expect(topicDelayExchange).to.be.ok;
        });

        it('should be a special message delay exchange', async function () {
            const topicDelayExchange = exchanges.find(x => x.name === DELAY_EXCHANGE_NAME);
            expect(topicDelayExchange).to.have.property('type');
            expect(topicDelayExchange.type).to.equal('x-delayed-message');
            expect(topicDelayExchange).to.have.property('arguments');
            expect(topicDelayExchange.arguments).to.have.property('x-delayed-type');
            expect(topicDelayExchange.arguments['x-delayed-type']).to.equal('topic');
        });

    });

    describe('delayed message, publishing without delay', function () {

        let queueAdapter;
        this.timeout(500);
        before('cleanup', cleanup);

        before('creating exchange and consuming', async function () {
            broker = createDelayableBroker();
            queueAdapter = broker.initQueue('testDelay');
            await queueAdapter.consume((message, { headers }) => {
                incomingMessages.push({ message, headers });
            }, 'test.*.delay.topic');
        });

        before('publishing', async function () {
            await queueAdapter.publishTo('test.no.delay.topic', 'hi there 1', { headers: { 'x-what': 'foo' } });
        });

        it('should receive the message immediately', function (done) {
            const handleMessage = sinon.stub();
            common.readDataFrom(incomingMessages, handleMessage, err => {
                try {
                    expect(err).to.not.be.ok;
                    expect(handleMessage.callCount).to.equal(1);
                    const firstCallArg = handleMessage.firstCall.args[0];
                    expect(firstCallArg).to.have.property('message');
                    expect(firstCallArg.message).to.equal('hi there 1');
                    expect(firstCallArg).to.have.property('headers');
                    expect(firstCallArg.headers).to.deep.equal({ 'x-what': 'foo' });
                    done();
                } catch (err) {
                    done(err);
                }
            });
        });
    });

    describe('delayed message, publish with delay', function () {
        let queueAdapter;

        before('cleanup', cleanup);

        before('creating exchange', async function () {
            broker = createDelayableBroker();
            queueAdapter = broker.initQueue('testDelay');
        });

        before('consuming', async function () {
            await queueAdapter.consume((message, { headers }) => {
                incomingMessages.push({ message, headers });
            }, 'test.*.delay.topic');
        });

        before('publishing', async function () {
            await queueAdapter.publishTo('test.no.delay.topic', 'hi there 2', {
                delay: 500,
                headers: {
                    'x-what': 'bar'
                },
            });
        });

        it('should not receive the message during delay', function (done) {
            const handleMessage = sinon.stub();
            common.readDataFrom(incomingMessages, handleMessage, errors => {
                try {
                    expect(errors).to.not.be.ok;
                    expect(handleMessage.callCount, 'message was not delayed').to.equal(0);
                    done();
                } catch (err) {
                    done(err);
                }
            }, 300);
        });

        it('should receive the message after delay', function (done) {
            const handleMessage = sinon.stub();
            common.readDataFrom(incomingMessages, handleMessage, errors => {
                try {
                    expect(errors).to.not.be.ok;
                    expect(handleMessage.callCount).to.equal(1);
                    const firstCallArg = handleMessage.firstCall.args[0];
                    expect(firstCallArg).to.have.property('message');
                    expect(firstCallArg.message).to.equal('hi there 2');
                    expect(firstCallArg).to.have.property('headers');
                    expect(firstCallArg.headers).to.deep.equal({
                        'x-delay': 500,
                        'x-what': 'bar'
                    })
                    done();
                } catch (err) {
                    done(err);
                }
            });
        });
    });

    describe('delayed message, publish with ms delay', function () {
        let queueAdapter;

        before('cleanup', cleanup);

        before('creating exchange', async function () {
            broker = createDelayableBroker();
            queueAdapter = broker.initQueue('testDelay');
        });

        before('consuming', async function () {
            await queueAdapter.consume((message, { headers }) => {
                incomingMessages.push({ message, headers });
            }, 'test.*.delay.topic');
        });

        before('publishing', async function () {
            await queueAdapter.publishTo('test.no.delay.topic', 'hi there 2', { delay: '0.4s' });
        });

        it('should receive the message after delay', function (done) {
            const handleMessage = sinon.stub();
            common.readDataFrom(incomingMessages, handleMessage, errors => {
                try {
                    expect(errors).to.not.be.ok;
                    expect(handleMessage.callCount).to.equal(1);
                    const firstCallArg = handleMessage.firstCall.args[0];
                    expect(firstCallArg).to.have.property('message');
                    expect(firstCallArg.message).to.equal('hi there 2');
                    expect(firstCallArg).to.have.property('headers');
                    expect(firstCallArg.headers).to.deep.equal({ 'x-delay': 400 })
                    done();
                } catch (err) {
                    done(err);
                }
            });
        });
    });
});