const Promise = require('bluebird');
const should = require('chai').should();
const Broker = require('..');
const url = 'amqp://localhost';
const common = require('./common');

describe('request-reply should: ', function () {
    it('send and receive via direct reply-to queue', async function () {
        let broker = new Broker({
            url,
            queues: {
                testReplyTo: {
                    name: 'test-reply-to',
                    requestReply: true
                }
            }
        });
        try {
            let serverReceived;
            let server = broker.initQueue('testReplyTo');
            console.log(`======================================= server: consuming from test-reply-to =======================================`);
            await server.consume(async (data, props) => {
                console.log(`======================================= server: received message =======================================`);
                serverReceived = JSON.parse(data);
                should.exist(props.replyTo);
                return { ok: 1 };
            });


            let client = broker.initQueue('testReplyTo');
            console.log(`======================================= client: PUBLISHING to test-reply-to =======================================`);
            let response = await client.publish({ the: 'entity' });

            should.exist(serverReceived, `message was not received`);
            serverReceived.should.deep.equal({ the: 'entity' });
            response.should.deep.equal({ ok: 1 });
            console.log(`=============================== SUCCESS ===============================`);
        } finally {
            await common.cleanup(broker, [], 'test-reply-to');
        }
    });

    it('publish multiple requests in parallel from the same queue wrapper', async function () {
        let broker = new Broker({
            url,
            queues: {
                testReplyTo: {
                    name: 'test-reply-to',
                    requestReply: true
                }
            }
        });
        try {
            let server = broker.initQueue('testReplyTo');
            await server.consume(async (data, props) => {
                return { ok: 1 };
            });

            let client = broker.initQueue('testReplyTo');
            await Promise.map(new Array(10).fill(1), async x => {
                let response = await client.publish({ the: 'entity' });
                response.should.deep.equal({ ok: 1 });
            });
        } finally {
            await common.cleanup(broker, [], 'test-reply-to');
        }
    });

    it('send and receive via direct reply-to queue without response', async function () {
        let broker = new Broker({
            url,
            queues: {
                testReplyTo: {
                    name: 'test-reply-to',
                    requestReply: true
                }
            }
        });
        try {
            let serverReceived;
            let server = broker.initQueue('testReplyTo');
            await server.consume(async (data, props) => {
                await Promise.delay(50);
                serverReceived = JSON.parse(data);
            });

            let client = broker.initQueue('testReplyTo');
            let response = await client.publish({ the: 'entity' });

            should.exist(serverReceived, `message was not received`);
            serverReceived.should.deep.equal({ the: 'entity' });

            should.not.exist(response);
        } finally {
            await common.cleanup(broker, [], 'test-reply-to');
        }
    });

});
