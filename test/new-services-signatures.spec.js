const chaiAsPromised = require('chai-as-promised');
const { expect } = require('chai').use(chaiAsPromised);
const _ = require('lodash');
const sinon = require('sinon');
const { Readable } = require('stream');

const tabitamqp = require('../');
const common = require('./common');

// const { INFO_LABELS } = tabitamqp;
// const { CONNECTION_CLOSED, CHANNEL_CLOSED, CHANNEL_ERROR } = tabitamqp.ERROR_LABELS;

const testConnectionOptions = {
    logError(label, errOrMessage, logOptions) {
        if (label === CHANNEL_CLOSED || label === CONNECTION_CLOSED) return;
        if (label === CHANNEL_ERROR && String(errOrMessage.message).match(/RESOURCE-LOCKED.*exclusive/)) return;
        console.error(errOrMessage instanceof Error ? errOrMessage : new Error(errOrMessage));
    },
    logInfo(label, message, meta) {
        if (label === INFO_LABELS.MESSAGE_PUBLISHED) return;
        if (label === INFO_LABELS.CONNECTION_CLOSED) return;
        if (meta) console.log(label, message, meta); else console.log(label, message);
    },
};

describe('Tabit AMQP Package', function() {

    describe('Wrapper basic functionality', function() {

        const incomingMessages = new Readable({ objectMode: true, read() { /* Do nothing because no source, we will push */ } });

        const randomNumber = _.random(0, 1000000000);
        let ackOneMessage;

        before(() => {
            tabitamqp.assignNewBroker('testConnection01', 'Test Connection 01', testConnectionOptions);
            tabitamqp.assignNewBroker('testConnection02', 'Test Connection 02', testConnectionOptions);
        });

        it('should fail on bad connection attempt', function() {
            return expect(tabitamqp.testConnection01.connect()).to.be.eventually.rejectedWith(/cannot connect/i);
        });
        it('should list failed connection missing details', function() {
            return expect(tabitamqp.testConnection01.connect({ vhost: '/foo', username: 'guest' })).to.be.eventually.rejectedWith(/password,hostname,exchangeName/i);
        });
        it('should connect successfully and open queue', async function() {
            await tabitamqp.testConnection01.connect({ username: 'guest', password: 'guest', hostname: 'localhost', vhost: '/', exchange_name: 'amq.topic' }, [
                {
                    queueName: 'test-amqp-package',
                    routingKey: 'ros.test',
                    options: {
                        noBatch: true,
                        exclusive: true,
                    },
                    handler: async (msg, headers, ack) => {
                        // console.log(`${Date.now()}: Message on base handler:`, msg);
                        incomingMessages.push({ msg, ack });
                    },
                    prefetch: 1,
                },
            ]);
        });
        it('should throw some error when trying to subscribe to the same exclusive queue', function() {
            return expect(tabitamqp.testConnection02.connect({ username: 'guest', password: 'guest', hostname: 'localhost', vhost: '/', exchange_name: 'amq.topic' }, [
                {
                    queue_name: 'test-amqp-package',
                    routingKey: 'ros.testik',
                    options: {
                        durable: false,
                        exclusive: true,
                    },
                    handler: async (msg, headers, ack) => await ack(),
                    prefetch: 1,
                },
            ])).to.be.eventually.rejectedWith(/RESOURCE-LOCKED.*exclusive/);
        });
        it('should get proper config', function() {
            const brokerConfig = tabitamqp.testConnection01.getBrokerConfig();
            expect(brokerConfig).to.be.ok;
            expect(brokerConfig).to.not.be.empty;
            expect(brokerConfig).to.have.property('testConnection01');
            expect(brokerConfig.testConnection01.name).to.equal('testConnection01');
        });
        it('should publish one message', async function() {
            await tabitamqp.testConnection01.topicPublish('ros.test', { theNumber: randomNumber });
        });
        it('should receive that message', function(done) {
            this.timeout(1000);
            common.readDataFrom(incomingMessages, ({ msg, ack }) => {

                ack();
                expect(msg).to.have.property('theNumber');
                expect(msg.theNumber).to.equal(randomNumber);

            }, done);
        });
        it('should publish more couple of messages', async function() {
            await tabitamqp.testConnection01.topicPublish('ros.test', { another: 1 });
            await tabitamqp.testConnection01.topicPublish('ros.test', { another: 2 });
            await tabitamqp.testConnection01.topicPublish('ros.test', { another: 3 });
            await tabitamqp.testConnection01.topicPublish('ros.test', { another: 4 });
        });
        it('should receive the first and not the others, for a whole second, because prefetch 1', function(done) {
            const handlerThatNotAck = sinon.spy(function handler({ msg, ack }) { ackOneMessage = ack; });
            common.readDataFrom(incomingMessages, handlerThatNotAck, errors => {
                try {
                    expect(errors).to.be.undefined;
                    expect(handlerThatNotAck.callCount).to.eq(1);
                    expect(handlerThatNotAck.firstCall.args[0]).to.have.property('msg');
                    expect(handlerThatNotAck.firstCall.args[0].msg).to.deep.eq({ another: 1 });
                    done();
                } catch (err) { done(err); }
            }, 1000);
        });
        it('should ack the first and then receive the others and only those, at the next second', function(done) {
            const handlerThatAck = sinon.spy(function handler({ msg, ack }) { ack(); });
            expect(ackOneMessage).to.be.a('function');
            common.readDataFrom(incomingMessages, handlerThatAck, () => {
                try {
                    expect(handlerThatAck.called).to.be.ok;
                    expect(handlerThatAck.firstCall.args[0]).to.have.property('msg');
                    expect(handlerThatAck.firstCall.args[0].msg).to.deep.eq({ another: 2 });
                    expect(handlerThatAck.callCount).to.eq(3);
                    done();
                } catch (err) { done(err); }
            }, 1000);
            ackOneMessage();
        });

        after(async function() {
            if (tabitamqp.testConnection01) await tabitamqp.testConnection01.disconnect();
            if (tabitamqp.testConnection02) await tabitamqp.testConnection02.disconnect();
        });

    });

    describe.skip('Multi exchanges', function() {

        const incomingMessages1 = new Readable({ objectMode: true, read() { /* Do nothing because no source, we will push */ } });
        const incomingMessages2 = new Readable({ objectMode: true, read() { /* Do nothing because no source, we will push */ } });

        before(() => {
            tabitamqp.assignNewBroker('multiExchange', 'Multi exchange test connection', testConnectionOptions);
        });

        it('should connect successfully, assert 2 exchanges and subscribe', async function() {
            await tabitamqp.multiExchange.connect({
                username: 'guest',
                password: 'guest',
                hostname: 'localhost',
                vhost: '/',
                exchange_name: 'test.exchange.1',
            },
            [
                {
                    queueName: 'test-1-amqp-package',
                    routingKey: 'routing.test',
                    exchangeName: 'test.exchange.1',
                    options: {
                        noBatch: true,
                        exclusive: true,
                    },
                    handler: async (msg, headers, ackOrNack) => {
                        // console.log('Message on base handler:', msg);
                        incomingMessages1.push({ msg, ack: ackOrNack });
                    },
                },
                {
                    queueName: 'test-2-amqp-package',
                    routingKey: 'routing.test',
                    exchangeName: 'test.exchange.2',
                    options: {
                        noBatch: true,
                        exclusive: true,
                    },
                    handler: async (msg, headers, ackOrNack) => {
                        // console.log('Message on base handler:', msg);
                        incomingMessages2.push({ msg, ack: ackOrNack });
                    },
                },
            ]);
        });
        it('should push to exchange 2', async function() {
            await tabitamqp.multiExchange.topicPublish('routing.test', { multix: 'yea' }, { exchangeName: 'test.exchange.2' });
        });
        it('should receive the message on exchange 2', function(done) {
            this.timeout(1000);
            common.readDataFrom(incomingMessages2, ({ msg, ack }) => {

                ack();
                expect(msg).to.deep.equal({ multix: 'yea' });

            }, done);
        });
        it('should push again to exchange 2 for a whole second', async function() {
            await tabitamqp.multiExchange.topicPublish('routing.test', { multix: 'yea' }, { exchangeName: 'test.exchange.2' });
        });
        it('should not receive the message on exchange 1 for a whole second', function(done) {
            const handler = sinon.spy(function ({ msg, ack }) { ack(); });
            common.readDataFrom(incomingMessages1, handler, () => {
                try {
                    expect(handler.callCount).to.equal(0);
                    done();
                } catch (err) { done(err); }
            }, 1000);
        });

        after(async function() {
            if (tabitamqp.multiExchange) await tabitamqp.multiExchange.disconnect();
        });

    });
});
