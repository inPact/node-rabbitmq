const _ = require('lodash');
const should = require('chai').should();
const Broker = require('..');
const url = 'amqp://localhost';
const API_URL = 'http://localhost:15672/api';
const superagent = require('superagent');
const common = require('./common');
const API_AUTH_ARGS = ['guest', 'guest'];

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
            await broker.createQueue('test', {
                queueName: 'custom-name',
                sectionOverride: {
                    exchange: {
                        name: 'custom-exchange-name'
                    }
                }
            }).consume(x => x);

            let response = await getFromApi('exchanges');
            let exchangeNames = response.map(x => x.name);
            exchangeNames.should.include('custom-exchange-name', exchangeNames);
            exchangeNames.should.not.include('test');
        } finally {
            await common.cleanup(broker, ['custom-name', 'test'], 'custom-exchange-name', 'test');
        }
    });

    it('build and bind nested dead-letter queues', async function () {
        let broker = new Broker({
            url,
            queues: {
                test: {
                    name: 'main',
                    deadLetter: {
                        dlx: 'retry-main-exchange',
                        dlq: 'retry-main',
                        deadLetter: {
                            dlx: 'failed-main-exchange',
                            dlq: 'failed-main',
                        },
                    },
                    exchange: {
                        name: 'main-x',
                        type: 'fanout'
                    }
                },
            }
        });
        try {
            await broker.createQueue('test').consume(x => x);

            let exchanges = await getFromApi('exchanges');
            let queues = await getFromApi('queues');

            let exchangeNamess = exchanges.map(x => x.name);
            exchangeNamess.should.include('main-x', exchangeNamess);
            exchangeNamess.should.include('retry-main-exchange', exchangeNamess);
            exchangeNamess.should.include('failed-main-exchange', exchangeNamess);

            let queueNamess = queues.map(x => x.name);
            queueNamess.should.include('main', exchangeNamess);
            queueNamess.should.include('retry-main', exchangeNamess);
            queueNamess.should.include('failed-main', exchangeNamess);

            let mainQueue = queues.find(x => x.name === 'main');
            mainQueue.arguments.should.include({ "x-dead-letter-exchange": "retry-main-exchange" });

            let retryQueue = queues.find(x => x.name === 'retry-main');
            retryQueue.arguments.should.include({ "x-dead-letter-exchange": "failed-main-exchange" });
        } finally {
            await common.cleanup(broker, ['main-x', 'retry-main-exchange'], 'main', 'retry-main');
        }
    });

    it('build and bind dead-letter queues with overrides', async function () {
        let broker = new Broker({
            url,
            queues: {
                test: {
                    name: 'main',
                    deadLetter: {
                        dlx: 'retry-main-exchange',
                        dlq: 'retry-main',
                        deadLetter: {
                            dlx: 'failed-main-exchange',
                            dlq: 'failed-main',
                        },
                    },
                    exchange: {
                        name: 'main-x',
                        type: 'fanout'
                    }
                },
            }
        });
        try {
            await broker.createQueue('test').consume(x => x);
            await broker.createQueue('test', { queueName: 'overriden' }).consume(x => x);

            let exchanges = await getFromApi('exchanges');
            let queues = await getFromApi('queues');

            let exchangeNamess = exchanges.map(x => x.name);
            exchangeNamess.should.include('main-x', exchangeNamess);
            exchangeNamess.should.include('retry-main-exchange', exchangeNamess);
            exchangeNamess.should.include('failed-main-exchange', exchangeNamess);

            let queueNamess = queues.map(x => x.name);
            queueNamess.should.include('main', exchangeNamess);
            queueNamess.should.include('overriden', exchangeNamess);
            queueNamess.should.include('retry-main', exchangeNamess);
            queueNamess.should.include('failed-main', exchangeNamess);

            let mainQueue = queues.find(x => x.name === 'main');
            mainQueue.arguments.should.include({ "x-dead-letter-exchange": "retry-main-exchange" });

            let overridenQueue = queues.find(x => x.name === 'overriden');
            overridenQueue.arguments.should.include({ "x-dead-letter-exchange": "retry-main-exchange" });

            let retryQueue = queues.find(x => x.name === 'retry-main');
            retryQueue.arguments.should.include({ "x-dead-letter-exchange": "failed-main-exchange" });
        } finally {
            await common.cleanup(broker, ['main-x', 'retry-main-exchange'], 'main','overriden', 'retry-main');
        }
    });

});

async function getFromApi(...pathParts) {
    let response = await superagent.get(`${API_URL}/${_.join(pathParts, '/')}`).auth(...API_AUTH_ARGS);
    return response.body;
}