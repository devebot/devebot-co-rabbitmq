'use strict';

var Devebot = require('devebot');
var Promise = Devebot.require('bluebird');
var lodash = Devebot.require('lodash');
var assert = require('chai').assert;
var expect = require('chai').expect;
var faker = require('faker');
var util = require('util');
var debugx = require('debug')('devebot:co:rabbitmq:rabbitmqHandler:test:swallow');
var debug0 = require('debug')('devebot:co:rabbitmq:rabbitmqHandler:test:swallow:timer');
var RabbitmqHandler = require('../../lib/bridges/rabbitmq-handler');
var appCfg = require('./app-configuration');
var bogen = require('./big-object-generator');
var Loadsync = require('loadsync');

describe('handler-stream-mutex:', function() {

	describe('stream/produce with mutex', function() {
		var FIELD_NUM = 10000;
		var CONST_TOTAL = 20;
		var CONST_TIMEOUT = 100;
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
					assert.equal(message.code, count);
					check.splice(check.indexOf(message.code), 1);
					debugx.enabled && debugx('Message #%s', message.code);
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
				}, 700);
				debugx.enabled && debugx('swallow() - start');
				return handler.pull(bos);
			}).then(function() {
				debugx.enabled && debugx('swallow() - done');
			}).catch(function(err) {
				debugx.enabled && debugx('swallow() - error');
				done(err);
			})
			this.timeout(10000000 + total*timeout*3);
		});
	});

	describe('stream/produce without mutex', function() {
		var FIELD_NUM = 10000;
		var CONST_TOTAL = 20;
		var CONST_TIMEOUT = 100;
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
				}, 700);
				debugx.enabled && debugx('swallow() - start');
				return handler.pull(bos);
			}).then(function() {
				debugx.enabled && debugx('swallow() - done');
			}).catch(function(err) {
				debugx.enabled && debugx('swallow() - error');
				done(err);
			})
			this.timeout(10000000 + total*timeout*3);
		});
	});
});
