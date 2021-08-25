const _ = require('lodash');
const should = require('chai').should();
const common = require('./common');
const Promise = require('bluebird');

describe('connection-manager should: ', function () {
    afterEach(async function () {
        await common.cleanup();
    });

    it('reconnect consumers from multiple brokers after disconnect', async function () {
        let broker1 = common.createBrokerWithTestQueue({ name: 'test1' });
        let broker2 = common.createBrokerWithTestQueue({ name: 'test2' });

        let received1 = 0;
        let received2 = 0;

        await broker1.initQueue('test').consume(x => received1++);
        await broker2.initQueue('test').consume(x => received2++);

        await broker1.initQueue('test').publish({ the: 'entity' });
        await broker2.initQueue('test').publish({ the: 'entity' });

        await Promise.delay(100);
        received1.should.equal(1);
        received2.should.equal(1);

        console.log(`=============================== closing connections and waiting for recovery... ===============================`);
        let broker1Connection = await broker1.getConnection();
        let broker2Connection = await broker2.getConnection();

        // close connections simultaneously
        broker1Connection.close();
        broker2Connection.close();
        await Promise.delay(500);
        console.log(`=============================== done waiting. re-testing... ===============================`);

        await broker1.initQueue('test').publish({ the: 'entity' });
        await broker2.initQueue('test').publish({ the: 'entity' });

        await Promise.delay(100);
        received1.should.equal(2);
        received2.should.equal(2);
    });
});
