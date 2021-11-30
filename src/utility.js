const _ = require('lodash');
const ms = require('ms');

module.exports = {
    /**
     * Convert all ms-able fields from strings to milliseconds. This method updates {@param obj}.
     * @param obj - the object on which to update.
     * @param msFields - an array of fields that should be checked if they can be parsed by ms and then updated on {@param obj}.
     */
    setAllMilliseconds(obj, msFields){
        _.forEach(msFields, path => {
            let val = _.get(obj, path);
            if (typeof val === 'string')
                _.set(obj, path, ms(val));
        });

        return obj;
    }
}