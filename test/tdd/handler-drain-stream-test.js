'use strict';

var Devebot = require('devebot');
var Promise = Devebot.require('bluebird');
var lodash = Devebot.require('lodash');
var assert = require('chai').assert;
var expect = require('chai').expect;
var faker = require('faker');
var util = require('util');
var debugx = require('debug')('devebot:co:rabbitmq:rabbitmqHandler:test:stream');
var debug0 = require('debug')('devebot:co:rabbitmq:rabbitmqHandler:test:stream:timer');
var RabbitmqHandler = require('../../lib/bridges/rabbitmq-handler');
var appCfg = require('./app-configuration');
var bogen = require('./big-object-generator');
var Loadsync = require('loadsync');

describe('rabbitmq-handler:', function() {

	describe('drain-stream', function() {
		var FIELD_NUM = 10000;
		var CONST_TOTAL = 100;
		var CONST_TIMEOUT = 5;
		var handler;

		before(function() {
			handler = new RabbitmqHandler(appCfg.extend());
		});

		beforeEach(function(done) {
			handler.ready().then(function() {
				done();
			});
		});

		afterEach(function(done) {
			handler.destroy().then(function() {
				done();
			});
		});

		it('emit drain event if the produce() is overflowed (direct)', function(done) {
			var timeout = CONST_TIMEOUT;
			var total = CONST_TOTAL;
			var count = 0;
			var check = lodash.range(total);
			var bog = new bogen.BigObjectGenerator(FIELD_NUM, total, timeout);
			var ok = handler.consume(function(message, info, finish) {
				message = JSON.parse(message);
				check.splice(check.indexOf(message.code), 1);
				debugx.enabled && debugx('Message #%s', message.code);
				finish();
				if (++count >= total) {
					handler.checkChain().then(function(info) {
						assert.equal(info.messageCount, 0, 'Chain should be empty');
						debugx.enabled && debugx('Absent messages: ', JSON.stringify(check));
						done();
					});
				}
			}).then(function() {
				return handler.purgeChain();
			});
			ok.then(function() {
				debug0.enabled && debug0('Starting...');
				var sendData = function() {
					handler.outlet.removeAllListeners('drain');
					bog.next().then(function(data) {
						if (!data) {
							debug0.enabled && debug0('End');
							return;
						};
						handler.produce(data).then(function() {
							sendData();
						}).catch(function() {
							handler.outlet.on('drain', sendData);
						});
					})
				}
				sendData();
			}).catch(function(err) {
				done(err);
			})
			this.timeout(10000000 + total*timeout*3);
		});

		it('emit drain event if the produce() is overflowed (stream)', function(done) {
			var timeout = CONST_TIMEOUT;
			var total = CONST_TOTAL;
			var count = 0;
			var check = lodash.range(total);
			var bog = new bogen.BigObjectGenerator(FIELD_NUM, total, timeout);
			var ok = handler.consume(function(message, info, finish) {
				message = JSON.parse(message);
				check.splice(check.indexOf(message.code), 1);
				debugx.enabled && debugx('Message #%s', message.code);
				finish();
				if (++count >= total) {
					handler.checkChain().then(function(info) {
						assert.equal(info.messageCount, 0, 'Chain should be empty');
						debugx.enabled && debugx('Absent messages: ', JSON.stringify(check));
						done();
					});
				}
			}).then(function() {
				return handler.purgeChain();
			});
			ok.then(function() {
				var bos = new bogen.BigObjectStreamify(bog, {objectMode: true});
				debug0.enabled && debug0('Starting...');
				var writable = true;
				var doRead = function() {
					var data = bos.read();
					handler.produce(data).then(function() {
						writable = true;
					}).catch(function() {
						writable = false;
					});
				}
				bos.on('readable', function() {
					if(writable) {
						doRead();
					} else {
						handler.outlet.removeAllListeners('drain');
						handler.outlet.on('drain', doRead)
					}
				});
				bos.on('end', function() {
					debug0.enabled && debug0('End');
				});
			}).catch(function(err) {
				done(err);
			})
			this.timeout(10000000 + total*timeout*3);
		});
	});
});
