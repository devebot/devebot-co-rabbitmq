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

describe('RabbitmqHandler:', function() {

	describe('Constructor', function() {
		before(function() {
			checkSkip.call(this, 'constructor');
		});
	});

	describe('Workbench', function() {
		var handler = new RabbitmqHandler({
			host: 'amqp://master:zaq123edcx@192.168.56.56',
			exchangeType: 'direct',
			exchange: 'sample-exchange',
			routingKey: 'sample',
			queue: 'sample-queue',
			durable: true,
			noAck: false,
			consumerTag: 'Testing-Consumer'
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

		it('preserve the order of elements', function(done) {
			var index = 0;
			handler.consume(function(message, end) {
				message = JSON.parse(message);
				assert(message.code === index++);
				end();
				if (index >= 10) done();
			});
			var arr = generateRange(0, 10);
			arr.forEach(function(count) {
				handler.publish({ code: count, msg: 'Hello world' });
			});
		});

		it('push elements to queue massively', function(done) {
			var max = 5000;
			var idx = generateRange(0, max);
			var n0to9 = generateRange(0, 10);
			var count = 0;
			handler.consume(function(message, end) {
				message = JSON.parse(message);
				var pos = idx.indexOf(message.code);
				if (pos >= 0) idx.splice(pos, 1);
				end();
				count++;
				if (count >= max * 10) {
					assert(idx.length === 0);
					done();
				}
			}).then(function() {
				var arr = generateRange(0, max);
				Promise.reduce(arr, function(state, n) {
					return Promise.each(n0to9, function(k) {
						handler.publish({ code: (10*n + k), msg: 'Hello world' });
					}).delay(1);
				}, {});
			});
			this.timeout(60*max);
		});

		it('push large elements to queue', function(done) {
			var total = 10;
			var index = 0;
			var fields = generateFields(1000);
			handler.consume(function(message, end) {
				message = JSON.parse(message);
				assert(message.code === index++);
				end();
				if (index >= total) done();
			});
			var arr = generateRange(0, total);
			Promise.mapSeries(arr, function(count) {
				var randobj = generateObject(fields);
				randobj.code = count;
				return handler.publish(randobj).delay(1).then(function() {

				}).catch(function() {

				});
			});
		});
	});
});
