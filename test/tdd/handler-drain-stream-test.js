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

	describe('drain-stream', function() {
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

		it('emit drain event if the produce() is overflowed', function(done) {
			var timeout = 50;
			var total = 200;
			var count = 0;
			var check = lodash.range(total);
			var bog = new BigObjectGenerator(3000, total, timeout);
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
				var data = null;
				var sendData = function() {
					handler.outlet.removeAllListeners('drain');
					bog.next().then(function(data) {
						if (lodash.isEmpty(data)) return;
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
			this.timeout(1000 + total*timeout*2);
		});
	});
});

var faker = require('faker');
var BigObjectGenerator = function(fieldNum, total, timeout) {
	this.index = 0;
	this.total = total;
	this.fields = lodash.range(fieldNum).map(function(index) {
		return {
			name: 'field_' + index,
			type: 'string'
		}
	});
	this.next = function() {
		var obj = {};
		if (this.index >= this.total) return obj;
		this.fields.forEach(function(field) {
			obj[field.name] = faker.lorem.sentence();
		});
		obj.code = this.index;
		this.index++;
		return Promise.resolve(obj).delay(timeout);
	}
}
