const Promise = require('bluebird');
const should = require('chai').should();
const Broker = require('..');
const url = 'amqp://localhost';
const API_URL = 'http://localhost:15672/api';
const superagent = require('superagent');

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
                await cleanup(broker, 'test-basic', 'test-basic');
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
                await cleanup(broker, 'test-basic', 'q1', 'q2');
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
                await cleanup(broker, 'test', 'test');
            }
        });

        it.skip('send and receive via direct reply-to queue', async function () {
            let broker = new Broker({
                url,
                queues: {
                    testReplyTo: {
                        name: 'test-reply-to',
                        exchange: { name: 'test-reply-to' },
                        requestReply: true
                    }
                }
            });
            try {
                let serverReceived;
                let server = broker.createQueue('testReplyTo');
                await server.consume((data, props) => {
                    serverReceived = JSON.parse(data);
                    should.exist(props.replyTo)
                });



                await server.publish({ the: 'entity' });

                await Promise.delay(100);
                should.exist(serverReceived, `message was not received`);
                serverReceived.should.deep.equal({ the: 'entity' });
            } finally {
                await cleanup(broker, 'test-reply-to', 'test-reply-to');
            }
        });``
    });

    describe('topology should: ', function () {
        it('accept section overrides', async function () {
            let broker = new Broker({
                url,
                queues: {
                    test: {
                        name: 'test',
                        exchange: { name: 'test' }
                    }
                }
            });
            try {
                broker.createQueue('test', {
                    queueName: 'custom-name',
                    sectionOverride: {
                        exchange: {
                            name: 'custom-exchange-name'
                        }
                    }
                }).consume(x => x);

                await Promise.delay(2000);
                let response = await superagent.get(`${API_URL}/exchanges`).auth('guest', 'guest');
                let exchanges = response.body.map(x => x.name);
                exchanges.should.include('custom-exchange-name', exchanges);
                exchanges.should.not.include('test');
            } finally {
                await cleanup(broker, ['custom-name', 'test'], 'custom-exchange-name', 'test');
            }
        });
    })
});

async function cleanup(broker, exchanges, ...queues) {
    exchanges = [].concat(exchanges);
    let channel = await (await broker.getConnection()).createChannel();

    console.log(`=============================== CLEANUP: deleting queues ===============================`);
    await Promise.each(queues, async q => {
        if (q)
            await channel.deleteQueue(q);
    });

    console.log(`=============================== CLEANUP: deleting exchanges ===============================`);
    Promise.each(exchanges, async x => {
        if (x)
            await channel.deleteExchange(x);
    });

    console.log(`=============================== CLEANUP: finished ===============================`);
}