const Promise = require('bluebird');
const { Readable } = require('stream');
const _ = require('lodash');
const superagent = require('superagent');

const API_URL = 'http://localhost:15672/api';
const API_AUTH_ARGS = ['guest', 'guest'];

module.exports = {
    async cleanup(broker, exchanges, ...queues) {
        if (process.env.NO_TEST_CLEANUP)
            return;

        exchanges = [].concat(exchanges);
        let channel = await(await broker.getConnection()).createChannel();

        console.log(`=============================== CLEANUP: deleting queues ===============================`);
        await Promise.each(queues, async q => {
            if (q)
                await channel.deleteQueue(q);
        });

        console.log(`=============================== CLEANUP: deleting exchanges ===============================`);
        await Promise.each(exchanges, async x => {
            if (x)
                await channel.deleteExchange(x);
        });

        console.log(`=============================== CLEANUP: finished ===============================`);
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

    async getFromApi(...pathParts) {
        let response = await superagent.get(`${API_URL}/${_.join(pathParts, '/')}`).auth(...API_AUTH_ARGS);
        return response.body;
    },

};