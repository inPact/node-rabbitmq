const Promise = require('bluebird');
const { Readable } = require('stream');
const _ = require('lodash');
const superagent = require('superagent');
const Broker = require('..');

const API_URL = 'http://localhost:15672/api';
const API_AUTH_ARGS = ['guest', 'guest'];

module.exports = {
    async cleanup(broker) {
        if (process.env.NO_TEST_CLEANUP)
            return;

        if (broker)
            broker.disconnect();

        await this.deleteAllViaApi('queues');
        await this.deleteAllViaApi('exchanges');
        console.log(`=============================== CLEANUP: finished ===============================`);
    },

    async getFromApi(...pathParts) {
        let response = await superagent.get(`${API_URL}/${_.join(pathParts, '/')}`).auth(...API_AUTH_ARGS);
        return response.body;
    },

    async deleteAllViaApi(entityType, { addDefaultVhost = true } = {}) {
        let entities = await this.getFromApi(entityType);
        entities = entities.filter(x => x && x.name && x.name.indexOf('amq.') === -1);
        console.log(`=============================== CLEANUP: deleting ${entities.length} ${entityType} ===============================`);

        await Promise.each(entities, async entity => {
            await this.deleteViaApi(entityType, entity.name, { addDefaultVhost });
        });
    },

    async deleteViaApi(entityType, name, { addDefaultVhost = true } = {}) {
        let vhost = addDefaultVhost ? '%2F/' : '';
        let response = await superagent
            .delete(`${API_URL}/${entityType}/${vhost}${name}`)
            .auth(...API_AUTH_ARGS);

        return response.body;
    },

    /**
     * @param {Readable} readable NodeJS **paused** readable stream
     * @param {Function} handler The function which gets the data
     * @param {Function} cb Callback when finish consuming data
     * @param {Number} ms Fetch data for some milliseconds. If omitted will wait to first chunk and finish.
     */
    readDataFrom(readable, handler, cb, ms) {
        if (ms && _.isNumber(ms) && ms > 0) {

            const errors = [];

            // Fetch chunks for some time:
            const innerHandler = function innerHandler() {
                // console.log('Some data received (delay):', arguments[0]);
                try {
                    handler(...arguments);
                } catch (err) {
                    errors.push(err);
                }
            };
            readable.on('data', innerHandler);

            setTimeout(() => {
                readable.removeAllListeners();
                if (errors.length) cb(errors);
                else cb();
            }, ms);

        } else {
            // Fetch only one chunk:
            const innerHandler = function innerHandler() {
                readable.removeAllListeners();
                try {
                    handler(...arguments);
                    cb();
                } catch (err) {
                    cb(err);
                }
            };
            readable.once('data', innerHandler);
        }
    },

    /**
     * /**
     * Creates and returns a broker with one section named "test" containing a queue and exchange named "test",
     * with the specified exchange type
     * @param {String} [exchangeType] - if not provided, creates a RMQ direct queue
     * @param {String} [name] - the name to give both the queue and the exchange; defaults to "test"
     * @returns {exports}
     */
    createBrokerWithTestQueue({ exchangeType, name = 'test'} = {}) {
        return new Broker({
            url: 'amqp://localhost',
            queues: {
                test: {
                    name,
                    exchange: {
                        name,
                        type: exchangeType
                    }
                }
            }
        });
    }
};