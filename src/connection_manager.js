const amqp = require('amqplib');
const debug = require('debug')('tabit:infra:rabbit');
const EventEmitter = require('events').EventEmitter;
const utils = require('@tabit/utils');
const lock = utils.lock;

let connections = 0;

class ConnectionManager extends EventEmitter {
    /**
     * @param config - the queue configuration to assert.
     * @param logger
     */
    constructor(config, { logger = console } = {}) {
        super();

        this.config = config;
        this.logger = logger;
    }

    async getConnection(config) {
        return lock.getWithDoubleCheck(
            () => this.connection,
            'Queue.getConnection',
            () => this._connect(config),
            connection => this.connection = connection);
    }

    /** @private */
    _connect(config = this.config) {
        let options = config.heartbeat ? '?heartbeat=' + config.heartbeat : '';
        let url = (config.url || 'amqp://localhost') + options;
        debug(`Distributed queue: connecting to ${url}...`);

        return amqp.connect(url)
            .then(connection => {
                this.logger.info(`Distributed queue: connected to ${url}`);
                this.connection = connection;
                debug(`Distributed queue: ${++connections} open connections`);

                connection.on('close', () => {
                    this.logger.warn('Distributed queue: connection closed');
                    debug(`Distributed queue: ${--connections} open connections`);
                    delete this.connection;

                    this.emit('closed')
                });

                connection.on('error', e => {
                    this.logger.error('Distributed queue: connection error: ' + utils.errorToString(e));
                });

                return connection;
            });
    }

    dispose() {
        if (!this.connection)
            return;

        this.logger.warn('Distributed queue: destroying connection without consumer recovery');
        this.connection.removeAllListeners('close');
        this.connection.removeAllListeners('error');

        this.connection.close();
    }
}

module.exports = ConnectionManager;