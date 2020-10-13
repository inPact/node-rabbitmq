const Promise = require('bluebird');

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
        Promise.each(exchanges, async x => {
            if (x)
                await channel.deleteExchange(x);
        });

        console.log(`=============================== CLEANUP: finished ===============================`);
    }
};