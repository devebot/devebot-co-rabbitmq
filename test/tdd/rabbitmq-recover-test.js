'use strict';

var Loadsync = require('loadsync');
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
			Promise.all([
				handler.prepare(), handler.purgeChain(), handler.purgeTrash()
			]).then(function() {
				done();
			});
		});

		afterEach(function(done) {
			handler.destroy().then(function() {
				debugx.enabled && debugx('Handler has been destroyed');
				done();
			});
		});

		it('filter the failed processing data to trash (recycle-bin)', function(done) {
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
					handler.checkChain().then(function(info) {
						assert.equal(info.messageCount, 0, 'Chain should be empty');
						done();
					});
				}
			});
			var arr = generateRange(0, total);
			Promise.mapSeries(arr, function(count) {
				return handler.publish({ code: count, msg: 'Hello world' });
			});
			this.timeout(4000);
		});

		it('assure the total of recovered items in trash (recycle-bin)', function(done) {
			var total = 1000;
			var index = 0;
			var loadsync = new Loadsync([{
				name: 'testsync',
				cards: ['consume', 'recycle']
			}]);

			var code1 = [11, 21, 31, 41, 51, 61, 71, 81, 91, 99];
			handler.consume(function(message, info, finish) {
				message = JSON.parse(message);
				if (code1.indexOf(message.code) < 0) {
					finish();
				} else {
					finish('error');
				}
				if (++index >= (total + 3*10)) {
					handler.checkChain().then(function(info) {
						assert.equal(info.messageCount, 0, 'Chain should be empty');
						loadsync.check('consume', 'testsync');
					});
				}
			});

			var code2 = [];
			handler.recycle(function(message, info, finish) {
				message = JSON.parse(message);
				code2.push(message.code);
				if (code2.length >= 10) {
					handler.checkTrash().then(function(info) {
						assert.equal(info.messageCount, 0, 'Trash should be empty');
						loadsync.check('recycle', 'testsync');
					});
				}
				finish();
			});

			loadsync.ready(function(info) {
				assert.sameMembers(code1, code2, 'There are exactly ' + code1.length + ' failed items');
				setTimeout(done, 100);
			}, 'testsync');

			var arr = generateRange(0, total);
			Promise.mapSeries(arr, function(count) {
				return handler.publish({ code: count, msg: 'Hello world' });
			});

			this.timeout(4000);
		});
	});
});
