'use strict';

var Devebot = require('devebot');
var Promise = Devebot.require('bluebird');
var lodash = Devebot.require('lodash');
var assert = require('chai').assert;
var expect = require('chai').expect;
var faker = require('faker');
var util = require('util');
var debugx = require('debug')('devebot:co:rabbitmq:handler:test:stream');
var debug0 = require('debug')('devebot:co:rabbitmq:handler:test:stream:timer');
var RabbitmqHandler = require('../../lib/handler');
var appCfg = require('./app-configuration');
var bogen = require('./big-object-generator');
var Loadsync = require('loadsync');

describe('rabbitmq-handler:', function() {

	describe('publisher is overflowed:', function() {
		var FIELDS = bogen.FIELDS || 10000;
		var TOTAL = bogen.TOTAL || 100;
		var TIMEOUT = bogen.TIMEOUT || 0;
		var handler;

		before(function() {
			handler = new RabbitmqHandler(appCfg.extend());
		});

		beforeEach(function(done) {
			handler.ready().then(function() {
				return handler.purgeChain();
			}).then(function() {
				done();
			});
		});

		afterEach(function(done) {
			handler.destroy().then(function() {
				done();
			});
		});

		it('emit drain event if the produce() is overflowed', function(done) {
			var count = 0;
			var check = lodash.range(TOTAL);
			var bog = new bogen.BigObjectGenerator({numberOfFields: FIELDS, max: TOTAL, timeout: TIMEOUT});
			var hasDone = false;
			var ok = handler.consume(function(message, info, finish) {
				message = JSON.parse(message);
				check.splice(check.indexOf(message.code), 1);
				debugx.enabled && debugx('Message #%s', message.code);
				finish();
				if (++count >= TOTAL) {
					handler.checkChain().then(function(info) {
						assert.equal(info.messageCount, 0, 'Chain should be empty');
						debugx.enabled && debugx('Absent messages: ', JSON.stringify(check));
						!hasDone && done();
						hasDone = true;
					});
				}
			});
			ok.then(function() {
				debug0.enabled && debug0('Starting...');
				var sendData = function() {
					bog.next().then(function(data) {
						if (!data) {
							debug0.enabled && debug0('End');
							return;
						};
						handler.produce(data).then(function() {
							sendData();
						}).catch(function(err) {
							debugx.enabled && debugx('bog error: %s', JSON.stringify(err));
						});
					});
				}
				sendData();
			}).catch(function(err) {
				debugx.enabled && debugx('Error: %s', JSON.stringify(err));
				done(err);
			})
			this.timeout(10000000 + TOTAL*TIMEOUT*3);
		});

		it('emit drain event if the exhaust() is overflowed', function(done) {
			var count = 0;
			var check = lodash.range(TOTAL);
			var bog = new bogen.BigObjectGenerator({numberOfFields: FIELDS, max: TOTAL, timeout: TIMEOUT});
			var ok = handler.consume(function(message, info, finish) {
				message = JSON.parse(message);
				if (message) {
					check.splice(check.indexOf(message.code), 1);
					debugx.enabled && debugx('Message #%s', message.code);
					finish();
					if (++count >= TOTAL) {
						handler.checkChain().then(function(info) {
							assert.equal(info.messageCount, 0, 'Chain should be empty');
							debugx.enabled && debugx('Absent messages: ', JSON.stringify(check));
							done();
						});
					}
				}
			});
			ok.then(function() {
				var bos = new bogen.BigObjectStreamify(bog, {objectMode: true});
				debug0.enabled && debug0('Starting...');
				return handler.exhaust(bos);
			}).then(function() {
				debugx.enabled && debugx('exhaust() - done');
			}).catch(function(err) {
				debugx.enabled && debugx('exhaust() - error');
				done(err);
			})
			this.timeout(10000000 + TOTAL*TIMEOUT*3);
		});
	});
});
