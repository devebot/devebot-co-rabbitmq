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
var appCfg = require('./app-configuration');
var Loadsync = require('loadsync');

describe('rabbitmq-handler:', function() {

	describe('recycle', function() {
		var handler;

		before(function() {
			handler = new RabbitmqHandler(appCfg.extend({
				recycler: {
					queue: 'tdd-recoverable-trash',
					durable: true,
					noAck: false,
					redeliveredCountName: 'x-redelivered-count',
					redeliveredLimit: 3
				}
			}));
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

		it('filter the failed consumeing data to trash (recycle-bin)', function(done) {
			var total = 1000;
			var index = 0;
			var codes = [11, 21, 31, 41, 51, 61, 71, 81, 91, 99];
			var ok = handler.consume(function(message, info, finish) {
				message = JSON.parse(message);
				if (codes.indexOf(message.code) < 0) {
					finish();
				} else {
					finish('error');
				}
				if (++index >= (total + 3*codes.length)) {
					handler.checkChain().then(function(info) {
						assert.equal(info.messageCount, 0, 'Chain should be empty');
						done();
					});
				}
			}).then(function() {
				return handler.purgeChain();
			}).then(function() {
				return handler.purgeTrash();
			});
			ok.then(function() {
				Promise.mapSeries(lodash.range(total), function(count) {
					return handler.produce({ code: count, msg: 'Hello world' });
				});
			})
			this.timeout(5*total);
		});

		it('assure the total of recovered items in trash (recycle-bin)', function(done) {
			var total = 1000;
			var index = 0;
			var loadsync = new Loadsync([{
				name: 'testsync',
				cards: ['consume', 'recycle']
			}]);

			var code1 = [11, 21, 31, 41, 51, 61, 71, 81, 91, 99];
			var ok1 = handler.consume(function(message, info, finish) {
				message = JSON.parse(message);
				if (code1.indexOf(message.code) < 0) {
					finish();
				} else {
					finish('error');
				}
				if (++index >= (total + 3*code1.length)) {
					handler.checkChain().then(function(info) {
						assert.equal(info.messageCount, 0, 'Chain should be empty');
						loadsync.check('consume', 'testsync');
					});
				}
			}).then(function() {
				return handler.purgeChain();
			});

			var code2 = [];
			var ok2 = handler.recycle(function(message, info, finish) {
				message = JSON.parse(message);
				code2.push(message.code);
				if (code2.length >= code1.length) {
					handler.checkTrash().then(function(info) {
						assert.equal(info.messageCount, 0, 'Trash should be empty');
						loadsync.check('recycle', 'testsync');
					});
				}
				finish();
			}).then(function() {
				return handler.purgeTrash();
			});

			loadsync.ready(function(info) {
				assert.sameMembers(code1, code2, 'There are exactly ' + code1.length + ' failed items');
				setTimeout(done, 100);
			}, 'testsync');

			Promise.all([ok1, ok2]).then(function() {
				Promise.mapSeries(lodash.range(total), function(count) {
					return handler.produce({ code: count, msg: 'Hello world' });
				});
			});
			this.timeout(5*total);
		});
	});
});
