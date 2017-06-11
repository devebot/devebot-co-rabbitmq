'use strict';

var Devebot = require('devebot');
var Promise = Devebot.require('bluebird');
var lodash = Devebot.require('lodash');
var assert = require('chai').assert;
var expect = require('chai').expect;
var faker = require('faker');
var util = require('util');
var debugx = require('debug')('devebot:co:rabbitmq:rabbitmqHandler:test:exhaust');
var debug0 = require('debug')('devebot:co:rabbitmq:rabbitmqHandler:test:exhaust:timer');
var RabbitmqHandler = require('../../lib/bridges/rabbitmq-handler');
var appCfg = require('./app-configuration');
var bogen = require('./big-object-generator');
var Loadsync = require('loadsync');

describe('handler-stream-mutex:', function() {

	describe('stream/produce with mutex:', function() {
		var FIELD_NUM = 10000;
		var CONST_TOTAL = 100;
		var CONST_TIMEOUT = 0;
		var handler;

		before(function() {
			handler = new RabbitmqHandler(appCfg.extend({
				exchangeMutex: true
			}));
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

		it('prevent publish any message to pumping mutex stream', function(done) {
			var timeout = CONST_TIMEOUT;
			var total = CONST_TOTAL;
			var count = 0;
			var check = lodash.range(total);
			var bog = new bogen.BigObjectGenerator({numberOfFields: FIELD_NUM, max: total, timeout: timeout});
			var bo9 = new bogen.BigObjectGenerator({numberOfFields: FIELD_NUM, min: total, max: total+1, timeout: 0});
			var ok = handler.consume(function(message, info, finish) {
				message = JSON.parse(message);
				if (message) {
					debugx.enabled && debugx('message.code#%s / count: %s', message.code, count);
					assert.equal(message.code, count);
					check.splice(check.indexOf(message.code), 1);
					finish();
					if (++count >= (total + 1)) {
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
				setTimeout(function() {
					bo9.next().then(function(data) {
						debugx.enabled && debugx('produce() - inserting data');
						handler.produce(data).then(function() {
							debugx.enabled && debugx('produce() - data inserted');
						});
					});
				}, Math.round(CONST_TIMEOUT * CONST_TOTAL / 2));
				debugx.enabled && debugx('exhaust() - start');
				return handler.exhaust(bos);
			}).then(function() {
				debugx.enabled && debugx('exhaust() - done');
			}).catch(function(err) {
				debugx.enabled && debugx('exhaust() - error');
				done(err);
			})
			this.timeout(10000000 + total*timeout*3);
		});

		it('two exclusive mutex streams', function(done) {
			var timeout = CONST_TIMEOUT;
			var total = CONST_TOTAL;
			var count = 0;
			var check = lodash.range(2*total);
			var bog1 = new bogen.BigObjectGenerator({numberOfFields: FIELD_NUM, max: total, timeout: timeout});
			var bog2 = new bogen.BigObjectGenerator({numberOfFields: FIELD_NUM, min: total, max: 2*total, timeout: 0});
			var ok = handler.consume(function(message, info, finish) {
				message = JSON.parse(message);
				if (message) {
					assert.equal(message.code, count);
					check.splice(check.indexOf(message.code), 1);
					debugx.enabled && debugx('Message #%s', message.code);
					finish();
					if (++count >= (2*total)) {
						handler.checkChain().then(function(info) {
							assert.equal(info.messageCount, 0, 'Chain should be empty');
							debugx.enabled && debugx('Absent messages: ', JSON.stringify(check));
							done();
						});
					}
				}
			});
			ok.then(function() {
				var bos1 = new bogen.BigObjectStreamify(bog1, {objectMode: true});
				var bos2 = new bogen.BigObjectStreamify(bog2, {objectMode: true});
				debug0.enabled && debug0('Starting...');
				debugx.enabled && debugx('exhaust() - start');
				return Promise.all([
					handler.exhaust(bos1),
					handler.exhaust(bos2)
				])
			}).then(function() {
				debugx.enabled && debugx('exhaust() - done');
			}).catch(function(err) {
				debugx.enabled && debugx('exhaust() - error');
				done(err);
			})
			this.timeout(10000000 + total*timeout*3);
		});
	});

	describe('stream/produce without mutex:', function() {
		var FIELD_NUM = 10000;
		var CONST_TOTAL = 20;
		var CONST_TIMEOUT = 0;
		var handler;

		before(function() {
			handler = new RabbitmqHandler(appCfg.extend({
				exchangeMutex: false
			}));
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

		it('some messages is inserted to stream if mutex is not enabled', function(done) {
			var timeout = CONST_TIMEOUT;
			var total = CONST_TOTAL;
			var count = 0;
			var check = lodash.range(total);
			var successive = true;
			var bog = new bogen.BigObjectGenerator({numberOfFields: FIELD_NUM, max: total, timeout: timeout});
			var bo9 = new bogen.BigObjectGenerator({numberOfFields: FIELD_NUM, min: total, max: total+1, timeout: 0});
			var ok = handler.consume(function(message, info, finish) {
				message = JSON.parse(message);
				if (message) {
					if (message.code !== count) successive = false;
					check.splice(check.indexOf(message.code), 1);
					debugx.enabled && debugx('Message #%s', message.code);
					finish();
					if (++count >= (total + 1)) {
						handler.checkChain().then(function(info) {
							assert.equal(successive, false);
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
				setTimeout(function() {
					bo9.next().then(function(data) {
						debugx.enabled && debugx('produce() - inserting data');
						handler.produce(data).then(function() {
							debugx.enabled && debugx('produce() - data inserted');
						});
					});
				}, Math.round(CONST_TIMEOUT * CONST_TOTAL / 2));
				debugx.enabled && debugx('exhaust() - start');
				return handler.exhaust(bos);
			}).then(function() {
				debugx.enabled && debugx('exhaust() - done');
			}).catch(function(err) {
				debugx.enabled && debugx('exhaust() - error');
				done(err);
			})
			this.timeout(10000000 + total*timeout*3);
		});
	});
});
