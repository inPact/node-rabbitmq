const Promise = require('bluebird');
const should = require('chai').should();
const Broker = require('..');
const url = 'amqp://localhost';
const common = require('./common');

describe('messaging: ', function () {
    describe('send and receive should: ', function () {
        it('send and receive via direct queue', async function () {
            let broker = new Broker({
                url,
                queues: {
                    testBasic: {
                        name: 'test-basic',
                        exchange: { name: 'test-basic' }
                    }
                }
            });
            try {
                let received;
                let queue = broker.createQueue('testBasic');
                await queue.consume(data => received = JSON.parse(data));
                await queue.publish({ the: 'entity' });

                await Promise.delay(100);
                should.exist(received, `message was not received`);
                received.should.deep.equal({ the: 'entity' });
            } finally {
                await common.cleanup(broker, 'test-basic', 'test-basic');
            }
        });

        it('send and receive via basic queue', async function () {
            let broker = new Broker({
                url,
                queues: {
                    testBasic: { name: 'test-basic' }
                }
            });
            try {
                let received;
                let queue = broker.createQueue('testBasic');
                await queue.consume(data => received = JSON.parse(data));
                await queue.publishTo(null, JSON.stringify({ the: 'entity' }), { useBasic: true });

                await Promise.delay(100);
                should.exist(received, `message was not received`);
                received.should.deep.equal({ the: 'entity' });
            } finally {
                await common.cleanup(broker, 'test-basic', 'test-basic');
            }
        });

        it('send and receive via fanout queue', async function () {
            let broker = new Broker({
                url,
                queues: {
                    testBasic: {
                        name: 'test-basic',
                        exchange: {
                            name: 'test-basic',
                            type: 'fanout'
                        }
                    }
                }
            });
            try {
                let received = [];
                await broker.createQueue('testBasic', { queueName: 'q1' }).consume(data => {
                    received.push(JSON.parse(data))
                });
                await broker.createQueue('testBasic', { queueName: 'q2' }).consume(data => {
                    received.push(JSON.parse(data))
                });
                await broker.createQueue('testBasic').publish({ the: 'entity' });

                await Promise.delay(100);
                received.length.should.equal(2);
            } finally {
                await common.cleanup(broker, 'test-basic', 'q1', 'q2');
            }
        });

        it('reconnect consumers and publishers after disconnect', async function () {
            let broker = new Broker({
                url,
                queues: {
                    test: {
                        name: 'test',
                        exchange: {
                            name: 'test',
                            type: 'fanout'
                        }
                    }
                }
            });
            try {
                let received = [];
                await broker.createQueue('test', 'q1').consume(data => received.push(JSON.parse(data)));
                let pubQueue = broker.createQueue('test');
                await pubQueue.publish({ the: 'entity' });

                await Promise.delay(100); // wait for pub/sub
                received.length.should.equal(1);

                (await broker.getConnection()).close();
                await Promise.delay(500); // wait for close and reconnect
                await pubQueue.publish({ the: 'entity' });
                await Promise.delay(100); // wait for pub/sub
                received.length.should.equal(2);
            } finally {
                await common.cleanup(broker, 'test', 'test');
            }
        });
    });

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
                let server = broker.createQueue('testReplyTo');
                console.log(`======================================= server: consuming from test-reply-to =======================================`);
                await server.consume(async (data, props) => {
                    console.log(`======================================= server: received message =======================================`);
                    serverReceived = JSON.parse(data);
                    should.exist(props.replyTo)
                    return { ok: 1 };
                });


                let client = broker.createQueue('testReplyTo');
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

        it('publish multiple requests in parallel', async function () {
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
                let server = broker.createQueue('testReplyTo');
                await server.consume(async (data, props) => {
                    return { ok: 1 };
                });

                await Promise.map(new Array(10).fill(1), async x => {
                    let client = broker.createQueue('testReplyTo');
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
                let server = broker.createQueue('testReplyTo');
                await server.consume(async (data, props) => {
                    await Promise.delay(50);
                    serverReceived = JSON.parse(data);
                });

                let client = broker.createQueue('testReplyTo');
                let response = await client.publish({ the: 'entity' });

                should.exist(serverReceived, `message was not received`);
                serverReceived.should.deep.equal({ the: 'entity' });

                should.not.exist(response);
            } finally {
                await common.cleanup(broker, [], 'test-reply-to');
            }
        });

    });
});
