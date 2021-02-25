const common = require('./common');
const Broker = require('..');

const url = 'amqp://localhost';

describe('Messaging with broker', function() {
    describe('consume()', function() {
        it('should begin consume topic messages', async function() {

            const broker = new Broker({
                url,
                queues: {
                    testBasicTopic: {
                        name: 'test-basic-topic',
                        exchange: { name: 'test-basic', type: 'topic' }
                    }
                }
            });

            await broker.initQueueSection('testBasicTopic').consume(async (message) => {
                console.log('[X] Received:', message);
            }, 'system-*');

        });
        after(async function() {
            await common.cleanup(broker, 'test-basic', 'test-basic-topic');
        });
    });
});
