const Promise = require('bluebird');
const should = require('chai').should();
const QueueFactory = require('..');
const util = require('util');
const exec = util.promisify(require('child_process').exec);


describe('messaging should: ', function () {
    before(async function () {
    });

    after(async function () {
        process.exit();
    });

    it('send and retrieve via direct queue', async function () {
        let factory = new QueueFactory({
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
            let queue = factory.createQueue('testBasic');
            queue.consume(data => received = JSON.parse(data));
            await queue.publish({ the: 'entity' });

            await Promise.delay(100);
            should.exist(received);
            received.should.deep.equal({ the: 'entity' });
        } finally {
            console.log(`=============================== CLEANUP ===============================`);
            let channel = await (await factory.getConnection()).createChannel();
            await channel.deleteQueue('test-basic');
            await channel.deleteExchange('test-basic');
        }
    });

    it('send and retrieve via fanout queue', async function () {
        let factory = new QueueFactory({
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
            factory.createQueue('testBasic', 'q1').consume(data => {
                received.push(JSON.parse(data))
            });
            factory.createQueue('testBasic', 'q2').consume(data => {
                received.push(JSON.parse(data))
            });
            factory.createQueue('testBasic').publish({ the: 'entity' });

            await Promise.delay(100);
            received.length.should.equal(2);
        } finally {
            console.log(`=============================== CLEANUP ===============================`);
            let channel = await (await factory.getConnection()).createChannel();
            await channel.deleteQueue('q1');
            await channel.deleteQueue('q2');
            await channel.deleteExchange('test-basic');
        }
    });
});