'use strict';

var assert = require('chai').assert;
var expect = require('chai').expect;
var locks = require('locks');
var Devebot = require('devebot');
var Promise = Devebot.require('bluebird');
var lodash = Devebot.require('lodash');
var debug = Devebot.require('debug');
var debuglog = debug('devebot:co:rabbitmq:test:benchmark');
var Benchmark = require('benchmark');
var RabbitmqHandler = require('../../lib/bridges/handler');
var appCfg = require('../tdd/app-configuration');

const t = require('exectimer');
const Tick = t.Tick;
var tick = new Tick('subscriber');

var counter = { pub: 0, sub: 0, total: Number.MAX_SAFE_INTEGER }

var barrier = locks.createCondVariable(false);

barrier.wait(function conditionTest(value) {
	return value === true;
}, function whenConditionMet() {
	handler.destroy().then(function() {
		console.log('Handler has been finished');
	});
	var results = t.timers['subscriber'];
	var dur = results.duration() / 1000000000;
	console.log('Subscriber (%s/%s): %s ops/sec', counter.sub, dur, Math.round(counter.sub / dur));
});

var suite = new Benchmark.Suite('mix', {});

var config = appCfg.extend();

var handler = new RabbitmqHandler(config);

handler.consume(function(message, info, finish) {
	counter.sub++;
	if (counter.sub === 1) tick.start();
	if (counter.sub >= counter.total) {
		tick.stop();
		barrier.set(true);
	}
	message = JSON.parse(message);
	// console.log('received message.code: %s', message.code);
	finish();
});

suite.add('Publisher', {
	'defer': true,
	'fn': function(deferred) {
		handler.produce({
			code: counter.pub++,
			msg: 'Hello world'
		}).then(function() {
			deferred.resolve();
		});
	},
	'onStart': function() {
		console.log('Benchmark start ...');
	},
	'onComplete': function() {
		counter.total = counter.pub;
		console.log('Benchmark complete!');
	}
});

handler.ready().then(function() {
	return handler.purgeChain();
}).then(function() {
	suite.on('cycle', function(event) {
		console.log(String(event.target));
	})
	.run({ 'async': true });
});
