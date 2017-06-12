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

	describe('recycle() method:', function() {
		var handler;

		before(function() {
			handler = new RabbitmqHandler(appCfg.extend({
				recycler: {
					queueName: 'tdd-recoverable-trash',
					durable: true,
					noAck: false,
					redeliveredCountName: 'x-redelivered-count',
					redeliveredLimit: 3
				}
			}));
		});

		beforeEach(function(done) {
			Promise.all([
				handler.ready(), handler.purgeChain(), handler.purgeTrash()
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

		it('filter the failed consumeing data to trash (recycle-bin)', function(done) {
			var total = 1000;
			var index = 0;
			var codes = [11, 21, 31, 41, 51, 61, 71, 81, 91, 99];
			var hasDone = 0;
			var ok = handler.consume(function(message, info, finish) {
				message = JSON.parse(message);
				finish(codes.indexOf(message.code) < 0 ? undefined : 'error');
				if (++index >= (total + 3*codes.length)) {
					handler.checkChain().then(function(info) {
						assert.equal(info.messageCount, 0, 'Chain should be empty');
						(hasDone++ === 0) && done();
					});
				}
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
				finish(code1.indexOf(message.code) < 0 ? undefined : 'error');
				if (++index >= (total + 3*code1.length)) {
					handler.checkChain().then(function(info) {
						assert.equal(info.messageCount, 0, 'Chain should be empty');
						loadsync.check('consume', 'testsync');
					});
				}
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

	describe('garbage recovery:', function() {
		var handler;

		before(function() {
			handler = new RabbitmqHandler(appCfg.extend({
				recycler: {
					queueName: 'tdd-recoverable-trash',
					durable: true,
					noAck: false,
					redeliveredCountName: 'x-redelivered-count',
					redeliveredLimit: 3
				}
			}));
		});

		beforeEach(function(done) {
			Promise.all([
				handler.ready(), handler.purgeChain(), handler.purgeTrash()
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

		it('examine garbage items in trash (recycle-bin)', function(done) {
			var total = 1000;
			var index = 0;
			var loadsync = new Loadsync([{
				name: 'testsync',
				cards: ['consume']
			}]);

			var codes = [11, 21, 31, 41, 51, 61, 71, 81, 91, 99];
			handler.consume(function(message, info, finish) {
				message = JSON.parse(message);
				finish(codes.indexOf(message.code) < 0 ? undefined : 'error');
				++index;
				if (index == (total + 3*codes.length)) {
					handler.checkChain().then(function(info) {
						assert.equal(info.messageCount, 0, 'Chain should be empty');
						loadsync.check('consume', 'testsync');
					});
				}
				if (index > (total + 3*codes.length)) {
					debugx.enabled && debugx('Recovery message: %s', JSON.stringify(message));
					assert.equal(message.code, total);
					handler.checkChain().then(function(info) {
						assert.equal(info.messageCount, 0, 'Chain should be empty');
					});
				}
			}).then(function() {
				return Promise.mapSeries(lodash.range(total), function(count) {
					return handler.produce({ code: count, msg: 'Hello world' });
				});
			});

			loadsync.ready(function(info) {
				var msgcode;
				Promise.resolve().delay(50 * codes.length).then(function() {
					return handler.checkTrash().then(function(info) {
						debugx.enabled && debugx('Trash info: %s', JSON.stringify(info));
						assert.equal(info.messageCount, (codes.length-0), 'Trash should has ' + (codes.length-0) + ' items');
						return true;
					})
				}).then(function() {
					return handler.examine(function(msg, update) {
						var message = JSON.parse(msg.content.toString());
						debugx.enabled && debugx('Garbage message: %s', JSON.stringify(message));
						msgcode = message.code;
						assert.isTrue(codes.indexOf(msgcode) >= 0);
						assert.equal(codes[0], msgcode);
						update('nop');
					});
				}).then(function() {
					return handler.checkTrash().then(function(info) {
						debugx.enabled && debugx('Trash info: %s', JSON.stringify(info));
						assert.equal(info.messageCount, (codes.length-1), 'Trash should has ' + (codes.length-1) + ' items');
						return true;
					})
				}).then(function() {
					return handler.examine(function(msg, update) {
						var message = JSON.parse(msg.content.toString());
						debugx.enabled && debugx('Garbage message: %s', JSON.stringify(message));
						assert.equal(message.code, msgcode);
						update('discard');
					});
				}).then(function() {
					return handler.examine(function(msg, update) {
						update('discard');
					});
				}).then(function() {
					return handler.checkTrash().then(function(info) {
						debugx.enabled && debugx('Trash info: %s', JSON.stringify(info));
						assert.equal(info.messageCount, (codes.length-2), 'Trash should has ' + (codes.length-2) + ' items');
						return true;
					})
				}).then(function() {
					return handler.examine(function(msg, update) {
						var message = JSON.parse(msg.content.toString());
						debugx.enabled && debugx('Garbage message: %s', JSON.stringify(message));
						assert.equal(message.code, codes[2]);
						message.msg = 'I will survive';
						update('restore', {
							content: JSON.stringify(message)
						});
					});
				}).then(function() {
					return handler.checkTrash().then(function(info) {
						debugx.enabled && debugx('Trash info: %s', JSON.stringify(info));
						assert.equal(info.messageCount, (codes.length-2), 'Trash should has ' + (codes.length-2) + ' items');
						return true;
					})
				}).then(function() {
					return handler.examine(function(msg, update) {
						var message = JSON.parse(msg.content.toString());
						debugx.enabled && debugx('Garbage message: %s', JSON.stringify(message));
						assert.equal(message.code, codes[2]);
						message.code = total;
						update('recover', {
							content: JSON.stringify(message)
						});
					});
				}).then(function() {
					return handler.checkTrash().then(function(info) {
						debugx.enabled && debugx('Trash info: %s', JSON.stringify(info));
						assert.equal(info.messageCount, (codes.length-3), 'Trash should has ' + (codes.length-3) + ' items');
						return true;
					})
				}).then(function() {
					return handler.examine(function(msg, update) {
						var message = JSON.parse(msg.content.toString());
						debugx.enabled && debugx('Garbage message: %s', JSON.stringify(message));
						assert.equal(message.code, codes[3]);
						message.code = total+1;
						update('requeue', {
							content: JSON.stringify(message)
						});
					});
				}).then(function() {
					return handler.checkTrash().then(function(info) {
						debugx.enabled && debugx('Trash info: %s', JSON.stringify(info));
						assert.equal(info.messageCount, (codes.length-3), 'Trash should has ' + (codes.length-3) + ' items');
						return true;
					})
				}).then(function() {
					return Promise.mapSeries(lodash.range(4, 10), function(count) {
						return handler.examine(function(msg, update) {
							var message = JSON.parse(msg.content.toString());
							debugx.enabled && debugx('Garbage message: %s', JSON.stringify(message));
							assert.equal(message.code, codes[count]);
							update('discard');
						});
					});
				}).then(function() {
					return handler.checkTrash().then(function(info) {
						debugx.enabled && debugx('Trash info: %s', JSON.stringify(info));
						assert.equal(info.messageCount, (1), 'Trash should has ' + (1) + ' items');
						return true;
					})
				}).then(function() {
					return handler.examine(function(msg, update) {
						var message = JSON.parse(msg.content.toString());
						debugx.enabled && debugx('Garbage message: %s', JSON.stringify(message));
						assert.equal(message.code, total + 1);
						update('nop');
					});
				}).then(function() {
					done();
				});
			}, 'testsync');

			this.timeout(100*total);
		});
	});
});
