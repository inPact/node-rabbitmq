const consumeTimeout = require("./consume-timeout");

// TODO: commander
if (process.argv[2] === 'consume-timeout') {

    consumeTimeout().then(() => {
        console.log('Have a nice day!');
    }).catch(err => {
        console.error(err);
    });

} else {
    console.error('cli argument is unknown:', process.argv[2] || 'undefined');
}
