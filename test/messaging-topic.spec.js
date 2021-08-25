const { expect } = require('chai');
const { Readable } = require('stream');
const sinon = require('sinon');
const { EventEmitter } = require('events');

const common = require('./common');
const Broker = require('..');

const url = 'amqp://localhost';

describe('prefetch=1 should work as expected:', function () {
    const incomingMessages = new Readable({
        objectMode: true, read() { /* Do nothing because no source, we will push */
        }
    });
    const finishMessageEvents = new EventEmitter();

    const broker = new Broker({
        url,
        prefetch: 1,
        queues: {
            testBasicTopic: {
                name: 'test-basic-topic',
                exchange: { name: 'test-basic', type: 'topic' }
            }
        }
    });

    const queueSection = broker.initQueue('testBasicTopic');

    before(async function () {
        await queueSection.consume(async (message) => {
            const finished = new Promise(resolve => {
                finishMessageEvents.once('go', resolve);
            });
            incomingMessages.push(message);
            await finished;
        }, 'system.*');

        await queueSection.publishTo('system.15', 'Message 1');
        await queueSection.publishTo('system.16', 'Message 2');
        await queueSection.publishTo('system.17', 'Message 3');
        await queueSection.publishTo('system.18', 'Message 4');
    });

    it('should receive only the first one after a whole 2 seconds because of the first not yet finished', function (done) {
        const handleIncomingMessages = sinon.stub();
        common.readDataFrom(incomingMessages, handleIncomingMessages, errors => {
            try {
                expect(errors).to.be.undefined;
                expect(handleIncomingMessages.callCount).to.equal(1);
                expect(handleIncomingMessages.firstCall.firstArg).to.equal('Message 1');
                done();
            } catch (err) {
                done(err);
            }
        }, 200);
    });
    it('should receive the others, once acked (finished)', function (done) {
        const handleIncomingMessages = sinon.spy(message => {
            finishMessageEvents.emit('go');
        });
        common.readDataFrom(incomingMessages, handleIncomingMessages, errors => {
            try {
                expect(errors).to.be.undefined;
                expect(handleIncomingMessages.callCount).to.equal(3);
                expect(handleIncomingMessages.firstCall.firstArg).to.equal('Message 2');
                expect(handleIncomingMessages.secondCall.firstArg).to.equal('Message 3');
                expect(handleIncomingMessages.thirdCall.firstArg).to.equal('Message 4');
                done();
            } catch (err) {
                done(err);
            }
        }, 200);
        finishMessageEvents.emit('go');
    });

    after(async function () {
        await common.cleanup();
    });
});


