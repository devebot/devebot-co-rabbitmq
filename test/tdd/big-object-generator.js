'use strict';

var Devebot = require('devebot');
var Promise = Devebot.require('bluebird');
var lodash = Devebot.require('lodash');
var Readable = require('stream').Readable;
var util = require('util');
var faker = require('faker');

var helper = {};

var BigObjectGenerator = helper.BigObjectGenerator = function(params) {
	params = params || {};
	this.index = params.min || 0;
	this.total = params.max || 1000;
	this.fields = lodash.range(params.numberOfFields || 10).map(function(index) {
		return {
			name: 'field_' + index,
			type: 'string'
		}
	});
	this.next = function() {
		if (this.index >= this.total) return Promise.resolve(null);
		var obj = {};
		this.fields.forEach(function(field) {
			obj[field.name] = faker.lorem.sentence();
		});
		obj.code = this.index;
		this.index++;
		return Promise.resolve(obj).delay(params.timeout || 0);
	}
}

var BigObjectStreamify = helper.BigObjectStreamify = function(generator, options) {
	options = options || {};
	Readable.call(this, options);
	this.generator = generator;
}

util.inherits(BigObjectStreamify, Readable);

BigObjectStreamify.prototype._read = function() {
	var self = this;
	self.generator.next().then(function(obj) {
		if (obj === null) {
			self.emit('end');
		} else {
			self.push(obj);
		}
		return obj;
	}).catch(function(error) {
		self.emit('error', error);
	})
}

module.exports = helper;