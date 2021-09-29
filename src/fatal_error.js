module.exports = class FatalError extends Error {
    constructor(message) {
        super(message);
        this.retry = false;
    }
};