const _ = require('lodash');
const Publisher = require('../publisher');

module.exports = class DelayedPublisher extends Publisher {
    async publishTo(routingKey = '', message, { channel, basic, ...options } = {}) {
        if (options.delay)
            options = this._getAndVerifyDelayOptions(options);

        // purposefully omit the "basic" option in order to force "basic" publishing to run through the exchange and get delayed
        return super.publishTo(routingKey, message, { channel, ...options });
    }

    _getAndVerifyDelayOptions(options) {
        this._setAllMilliseconds(options);

        if (!_.isNumber(options.delay))
            throw new Error('options.delay must be a number');

        if (options.delay < 0)
            throw new Error('options.delay is negative, cannot travel to the past');

        return _.merge({}, _.omit(options, 'delay'), { headers: { 'x-delay': options.delay } });
    }
}