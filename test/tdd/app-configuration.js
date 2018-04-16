var lodash = require('devebot').require('lodash');

var baseCfg = {
	uri: process.env.OPFLOW_TDD_URI || 'amqp://master:zaq123edcx@opflow-broker-default?frameMax=0x1000',
	exchangeType: 'direct',
	exchangeName: 'tdd-recoverable-exchange',
	exchangeQuota: 16,
	routingKey: 'tdd-recoverable',
	consumer: {
		queueName: 'tdd-recoverable-queue',
		durable: true,
		noAck: false
	},
	recycler: {
		queueName: 'tdd-recoverable-trash',
		redeliveredCountName: 'redelivered-count',
		redeliveredLimit: 3
	}
};

module.exports = {
	extend: function(ext) {
		ext = ext || {};
		return lodash.merge({}, baseCfg, ext);
	}
};
