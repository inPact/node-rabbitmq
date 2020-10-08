const Promise = require('bluebird');
const should = require('chai').should();
const QueueAdapter = require('..');
const util = require('util');
const exec = util.promisify(require('child_process').exec);


describe('messaging should: ', function () {
    it('send and retrieve via direct queue', async function () {
        let adapter = new QueueAdapter({
            url: 'amqp://localhost',
            queues: {
                testBasic: {
                    name: 'test-basic',
                    exchange: { name: 'test-basic' }
                }
            }
        });
        try {
            let received;
            let queue = adapter.createQueue('testBasic');
            queue.consume(data => received = JSON.parse(data));
            await Promise.delay(1000);
            await queue.publish({ the: 'entity' });

            await Promise.delay(100);
            should.exist(received, `message was not received`);
            received.should.deep.equal({ the: 'entity' });
        } finally {
            console.log(`=============================== CLEANUP ===============================`);
            let channel = await (await adapter.getConnection()).createChannel();
            console.log(`=============================== CLEANUP: deleting queues ===============================`);
            await channel.deleteQueue('test-basic');
            console.log(`=============================== CLEANUP: deleting exchanges ===============================`);
            await channel.deleteExchange('test-basic');
            console.log(`=============================== CLEANUP: finished ===============================`);
        }
    });

    it('send and retrieve via fanout queue', async function () {
        let adapter = new QueueAdapter({
            url: 'amqp://localhost',
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
            adapter.createQueue('testBasic', 'q1').consume(data => {
                received.push(JSON.parse(data))
            });
            adapter.createQueue('testBasic', 'q2').consume(data => {
                received.push(JSON.parse(data))
            });
            await Promise.delay(1000);
            adapter.createQueue('testBasic').publish({ the: 'entity' });

            await Promise.delay(100);
            received.length.should.equal(2);
        } finally {
            let channel = await (await adapter.getConnection()).createChannel();
            console.log(`=============================== CLEANUP: deleting queues ===============================`);
            await channel.deleteQueue('q1');
            await channel.deleteQueue('q2');
            console.log(`=============================== CLEANUP: deleting exchanges ===============================`);
            await channel.deleteExchange('test-basic');
            console.log(`=============================== CLEANUP: finished ===============================`);
        }
    });

    it('reconnect consumers and publishers after disconnect', async function () {
        let adapter = new QueueAdapter({
            url: 'amqp://localhost',
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
            adapter.createQueue('test', 'q1').consume(data => received.push(JSON.parse(data)));
            await Promise.delay(1000); // wait for topology to finish building
            let pubQueue = adapter.createQueue('test');
            await pubQueue.publish({ the: 'entity' });

            await Promise.delay(100); // wait for pub/sub
            received.length.should.equal(1);

            (await adapter.getConnection()).close();
            await Promise.delay(500); // wait for close and reconnect
            await pubQueue.publish({ the: 'entity' });
            await Promise.delay(100); // wait for pub/sub
            received.length.should.equal(2);
        } finally {
            let channel = await (await adapter.getConnection()).createChannel();
            console.log(`=============================== CLEANUP: deleting queues ===============================`);
            await channel.deleteQueue('q1');
            await channel.deleteQueue('q2');
            console.log(`=============================== CLEANUP: deleting exchanges ===============================`);
            await channel.deleteExchange('test-basic');
            console.log(`=============================== CLEANUP: finished ===============================`);
        }
    });

});