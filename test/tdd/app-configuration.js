var lodash = require('devebot').require('lodash');

var baseCfg = {
	host: 'amqp://master:zaq123edcx@192.168.56.56',
	exchangeType: 'direct',
	exchange: 'tdd-recoverable-exchange',
	routingKey: 'tdd-recoverable',
	queue: 'tdd-recoverable-queue',
	durable: true,
	noAck: false
};

module.exports = {
	extend: function(ext) {
		ext = ext || {};
		return lodash.merge({}, baseCfg, ext);
	}
};
