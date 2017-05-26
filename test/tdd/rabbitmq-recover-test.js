'use strict';

var Devebot = require('devebot');
var Promise = Devebot.require('bluebird');
var lodash = Devebot.require('lodash');
var assert = require('chai').assert;
var expect = require('chai').expect;
var faker = require('faker');
var util = require('util');
var debugx = require('debug')('devebot:co:rabbitmq:rabbitmqHandler:test');
var RabbitmqHandler = require('../../lib/bridges/rabbitmq-handler');

var checkSkip = function(name) {
	if (process.env.TDD_EXEC && process.env.TDD_EXEC.indexOf(name) < 0) {
		this.skip();
	}
}

var generateRange = function(min, max) {
	var range = [];
	for(var i=min; i<max; i++) range.push(i);
	return range;
}

var generateFields = function(num) {
	return generateRange(0, num).map(function(index) {
		return {
			name: 'field_' + index,
			type: 'string'
		}
	});
}

var generateObject = function(fields) {
	var obj = {};
	fields = fields || {};
	fields.forEach(function(field) {
		obj[field.name] = faker.lorem.sentence();
	});
	return obj;
}

describe('rabbitmq-recover:', function() {

	describe('workbench', function() {
		var handler = new RabbitmqHandler({
			host: 'amqp://master:zaq123edcx@192.168.56.56',
			exchangeType: 'direct',
			exchange: 'tdd-recoverable-exchange',
			routingKey: 'tdd-recoverable',
			queue: 'tdd-recoverable-queue',
			durable: true,
			noAck: false,
			recycler: {
				queue: 'tdd-recoverable-trash',
				durable: true,
				noAck: false,
				redeliveredCountName: 'x-redelivered-count',
            	redeliveredLimit: 3
			}
		});

		before(function() {
			checkSkip.call(this, 'workbench');
		});

		beforeEach(function(done) {
			handler.prepare().then(function() {
				done();
			});
		});

		afterEach(function(done) {
			handler.destroy().then(function() {
				debugx.enabled && debugx('Handler has been destroyed');
				done();
			});
		});

		it('push large elements to queue', function(done) {
			var total = 1000;
			var index = 0;
			handler.consume(function(message, info, finish) {
				message = JSON.parse(message);
				if ([11, 21, 31, 41, 51, 61, 71, 81, 91, 99].indexOf(message.code) < 0) {
					finish();
				} else {
					finish('error');
				}
				if (++index >= (total + 3*10)) {
					setTimeout(function() {
						handler.countQueueMessages().then(function(messageCount) {
							if (messageCount == 0) done();
						});
					}, 100);
				}
			});
			var arr = generateRange(0, total);
			Promise.mapSeries(arr, function(count) {
				return handler.publish({ code: count, msg: 'Hello world' });
			});
			this.timeout(4000);
		});
	});
});
