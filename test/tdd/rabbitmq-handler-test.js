'use strict';

var assert = require('chai').assert;
var expect = require('chai').expect;
var util = require('util');
var debugx = require('debug')('devebot:co:rabbitmq:rabbitmqHandler:test');
var RabbitmqHandler = require('../../lib/bridges/rabbitmq-handler');

var checkSkip = function(name) {
	if (process.env.TDD_EXEC && process.env.TDD_EXEC.indexOf(name) < 0) {
		this.skip();
	}
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
			noAck: false
		});

		before(function() {
			checkSkip.call(this, 'workbench');
		});

		beforeEach(function(done) {
			done();
		});

		afterEach(function(done) {
			handler.destroy().then(function() {
				debugx.enabled && debugx('Handler has been destroyed');
				done();
			});
		});

		it('test 1', function(done) {
			handler.consume(function(message, end) {
				console.log('==@ Received message 1: %s', message);
				end();
				done();
			});
			handler.publish({ code: 1, msg: 'Hello world' });
			// setTimeout(done, 1000);
		});

		it('test 2', function(done) {
			handler.consume(function(message, end) {
				console.log('==@ Received message 2: %s', message);
				end();
				done();
			});
			handler.publish({ code: 2, msg: 'Hello world' });
			// setTimeout(done, 1000);
		});
	});
});
