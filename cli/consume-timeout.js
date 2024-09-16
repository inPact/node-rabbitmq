const RabbitMQBroker = require('../');

module.exports = async function consumeTimeout() {

    const testBroker = new RabbitMQBroker({
        url: `amqp://guest:guest@localhost/`,
        prefetch: 1,
        onTimeoutRequeueToTail: true,
        onTimeoutMaxRetries: 2,
        queues: {
            test: {
                name: 'test-temp-consumer-timeout',
                exclusive: true,
                exchange: {
                    name: 'amqp.topic',
                    type: 'topic',
                },
            },
        },
    });

    const queueSection = testBroker.initQueue('test');

    queueSection.setHandleTimeout(20 * 1000);

    const channelEvents = queueSection.getChannelEvents();

    channelEvents.on('channel-closed', channelDescriptor => {
        console.log(`Oh no! Channel ${channelDescriptor} was closed!`);
        // process.exit(1);
    });

    await queueSection.consume(
        async (rawMsgContent, properties, fields) => {
            console.log('Message arrived!', rawMsgContent.toString());
            await new Promise(r => setTimeout(r, 1000 * 60 * 3));
            console.log('Done!');
        }, 'test-routing-key'
    );

    process.on('SIGINT', () => {
        testBroker.disconnect();
    });

};
