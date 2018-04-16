'use strict';

var Devebot = require('devebot');
var Promise = Devebot.require('bluebird');
var lodash = Devebot.require('lodash');
var assert = require('chai').assert;
var expect = require('chai').expect;
var faker = require('faker');
var util = require('util');
var debugx = require('debug')('devebot:co:rabbitmq:handler:test:exhaust');
var debug0 = require('debug')('devebot:co:rabbitmq:handler:test:exhaust:timer');
var RabbitmqHandler = require('../../lib/handler');
var appCfg = require('./app-configuration');
var bogen = require('./big-object-generator');
var Loadsync = require('loadsync');

describe('handler-stream-mutex:', function() {

	describe('stream/produce with mutex:', function() {
		var FIELDS = bogen.FIELDS || 10000;
		var TOTAL = bogen.TOTAL || 100;
		var TIMEOUT = bogen.TIMEOUT || 0;
		var handler;

		before(function() {
			handler = new RabbitmqHandler(appCfg.extend({
				exchangeMutex: true
			}));
		});

		beforeEach(function(done) {
			handler.ready().then(function() {
				return handler.purgeInbox();
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
			var count = 0;
			var check = lodash.range(TOTAL);
			var bog = new bogen.BigObjectGenerator({numberOfFields: FIELDS, max: TOTAL, timeout: TIMEOUT});
			var bo9 = new bogen.BigObjectGenerator({numberOfFields: FIELDS, min: TOTAL, max: TOTAL+1, timeout: 0});
			var ok = handler.consume(function(message, info, finish) {
				message = JSON.parse(message);
				if (message) {
					debugx.enabled && debugx('message.code#%s / count: %s', message.code, count);
					assert.equal(message.code, count);
					check.splice(check.indexOf(message.code), 1);
					finish();
					if (++count >= (TOTAL + 1)) {
						handler.checkInbox().then(function(info) {
							assert.equal(info.messageCount, 0, 'Inbox should be empty');
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
				}, Math.round(TIMEOUT * TOTAL / 2));
				debugx.enabled && debugx('exhaust() - start');
				return handler.exhaust(bos);
			}).then(function() {
				debugx.enabled && debugx('exhaust() - done');
			}).catch(function(err) {
				debugx.enabled && debugx('exhaust() - error');
				done(err);
			})
			this.timeout(10000000 + TOTAL*TIMEOUT*3);
		});

		it('two exclusive mutex streams', function(done) {
			var count = 0;
			var check = lodash.range(2*TOTAL);
			var bog1 = new bogen.BigObjectGenerator({numberOfFields: FIELDS, max: TOTAL, timeout: TIMEOUT});
			var bog2 = new bogen.BigObjectGenerator({numberOfFields: FIELDS, min: TOTAL, max: 2*TOTAL, timeout: 0});
			var ok = handler.consume(function(message, info, finish) {
				message = JSON.parse(message);
				if (message) {
					assert.equal(message.code, count);
					check.splice(check.indexOf(message.code), 1);
					debugx.enabled && debugx('Message #%s', message.code);
					finish();
					if (++count >= (2*TOTAL)) {
						handler.checkInbox().then(function(info) {
							assert.equal(info.messageCount, 0, 'Inbox should be empty');
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
			this.timeout(10000000 + TOTAL*TIMEOUT*3);
		});
	});

	describe('stream/produce without mutex:', function() {
		var FIELDS = bogen.FIELDS || 10000;
		var TOTAL = bogen.TOTAL || 20;
		var TIMEOUT = bogen.TIMEOUT || 0;
		var handler;

		before(function() {
			handler = new RabbitmqHandler(appCfg.extend({
				exchangeMutex: false
			}));
		});

		beforeEach(function(done) {
			handler.ready().then(function() {
				return handler.purgeInbox();
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
			var count = 0;
			var check = lodash.range(TOTAL);
			var successive = true;
			var bog = new bogen.BigObjectGenerator({numberOfFields: FIELDS, max: TOTAL, timeout: TIMEOUT});
			var bo9 = new bogen.BigObjectGenerator({numberOfFields: FIELDS, min: TOTAL, max: TOTAL+1, timeout: 0});
			var ok = handler.consume(function(message, info, finish) {
				message = JSON.parse(message);
				if (message) {
					if (message.code !== count) successive = false;
					check.splice(check.indexOf(message.code), 1);
					debugx.enabled && debugx('Message #%s', message.code);
					finish();
					if (++count >= (TOTAL + 1)) {
						handler.checkInbox().then(function(info) {
							assert.equal(successive, false);
							assert.equal(info.messageCount, 0, 'Inbox should be empty');
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
				}, Math.round(TIMEOUT * TOTAL / 2));
				debugx.enabled && debugx('exhaust() - start');
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
