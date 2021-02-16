const Broker = require('./broker');

module.exports = class BrokerWrapper {

    constructor(handle, options) {
        this.name = (options && options.name) || 'Connection Name';
        this.brokerHandle = handle;
        this.defaultExchange = null;
        this.options = Object.assign({}, defaultOptions, options);

        this.connectionName = null;
        this.connection = null;

        this.logger = {
            error: safeFunction(this.options.logError),
            info: safeFunction(this.options.logInfo),
        };
        this.onBrokerError = safeFunction(this.options.onBrokerError);
        this.connected = false;
    }

    async connect(options, subscriptions) {

        if (this.connected) throw new Error('please don\'t connect() twice. For new connection assign a new Broker');

        try {
            validateConnectionDetails(options);
        } catch (err) {
            throw new Error(`Connection details not valid: ${err.message}`);
        }

        this.defaultExchange = (options.exchange_name || options.exchangeName);

        // this.connectionName = this.getDefaultConnectionName();

        const url = this.createConnectionUrlFromOptions(options);
        const brokerConfig = this.createConnectionConfiguration(url, options, subscriptions);

        const broker = new Broker(brokerConfig);

        this.connected = true;

        if (subscriptions && _.isArray(subscriptions) && subscriptions.length) {
            for (const subscriptionDetails of subscriptions) {

                // try {
                //     if (!firstConnection) await this.addSubscriptionConfig(subscriptionDetails);
                // } catch (err) {
                //     await rabbit.close(connectionName);
                //     throw err;
                // }

                // Rabbot / Foo-Foo-MQ wants us to declare the message handling before the actual subscription:
                // const queueName = this.getQueueNameFromSubscriptionDetails(subscriptionDetails);
                // this.attachHandlerBeforeSubscribe(queueName, subscriptionDetails.handler, subscriptionDetails.handlerContext);

                // Real subscribe
                await this.subscribeTo(subscriptionDetails);
            }
        }

        return this;

    }

    createConnectionUrlFromOptions(options) {
        return `amqp://${options.username}:${options.password}@${options.hostname}${vhost}`;
    }

    createConnectionConfiguration(url, options, subscriptions) {
        const config = { url, queues: { } };
        if (subscriptions && _.isArray(subscriptions) && subscriptions.length) {
            subscriptions.forEach(subscriptionDetails => {

                if (!isValidSubscriptionType(subscriptionDetails)) throw new Error(`Invalid subscription: ${JSON.stringify(subscriptionDetails)}`);
                const confQueue = this.getQueueConfigFromSubscription(subscriptionDetails);
                if (config.queues[confQueue.name]) throw new Error(`Cannot add queue '${confQueue.name}' twice on same broker`);
                config.queues[confQueue.name] = confQueue;

            });
        }
        return config;
        // return {
        //     url,
        //     queues: {
        //         test: {
        //             name: 'main',
        //             deadLetter: {
        //                 dlx: 'retry-main-exchange',
        //                 dlq: 'retry-main',
        //                 deadLetter: {
        //                     dlx: 'failed-main-exchange',
        //                     dlq: 'failed-main',
        //                 },
        //             },
        //             exchange: {
        //                 name: 'main-x',
        //                 type: 'fanout'
        //             }
        //         },
        //     }
        // };
    }

    getQueueConfigFromSubscription(subscriptionDetails) {
        const queueName = subscriptionDetails.queue_name || subscriptionDetails.queueName;
        const exchangeName = subscriptionDetails.exchangeName || this.defaultExchange;
        // const connectionName = subscriptionDetails.connectionName || this.brokerHandle;

        // if (subscriptionDetails.options && !_.isEmpty(subscriptionDetails.options)) {
        //     this.rascalConfig.vhosts[connectionName].queues[queueName].options = subscriptionDetails.options;
        // }

        if (!exchangeName) throw new Error(`cannot create bindings to routing key: '${subscriptionDetails.routingKey}', cannot determine exchange name`);

        return { name: queueName, exchange: { name: exchangeName } };

        // if (subscriptionDetails.routingKey) {
        //     if (!this.rascalConfig.vhosts[connectionName].bindings) this.rascalConfig.vhosts[connectionName].bindings = {};
        //     this.rascalConfig.vhosts[connectionName].bindings[`${queueName}-bind`] = {
        //         source: exchangeName,
        //         destination: queueName,
        //         bindingKey: subscriptionDetails.routingKey,
        //     };
        // }
    }

    getQueueNameFromSubscriptionDetails(subscriptionDetails) {
        // Backwards compatibility here:
        return (subscriptionDetails && (subscriptionDetails.queue_name || subscriptionDetails.queueName)) || null;
    }

    async subscribeTo(subscriptionDetails) {
        await queue.consume(data => received = JSON.parse(data), topic);
    }

};

function validateConnectionDetails(details) {
    if (!details || _.isEmpty(details)) throw new Error('Cannot connect to RabbitMQ without any connection details');
    let missingDetails = [];
    if (!details.username) missingDetails.push('username');
    if (!details.password) missingDetails.push('password');
    if (!details.hostname) missingDetails.push('hostname');
    if (!details.vhost) missingDetails.push('vhost');
    if (!details.exchange_name && !details.exchangeName) missingDetails.push('exchangeName');
    if (missingDetails.length) throw new Error('Missing details for RabbitMQ connection: ' + missingDetails.join());
    return true;
};

function isValidSubscriptionType(subscription) {

    return (
        (subscription.queue_name || subscription.queueName) &&
        (
            subscription.handler &&
            _.isFunction(subscription.handler) &&
            subscription.handler instanceof Object.getPrototypeOf(async function() {}).constructor
        )
    );

}
