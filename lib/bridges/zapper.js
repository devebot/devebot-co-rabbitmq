'use strict';

var Buffer = global.Buffer || require('buffer').Buffer;
var misc = {};

misc.isBoolean = function(val) {
  return (typeof val === 'boolean');
}

misc.isFunction = function(val) {
  return (typeof val === 'function');
}

misc.isNumber = function(val) {
  return (typeof val === 'number');
}

misc.isInteger = function(val) {
  return Number.isInteger(val);
}

misc.isPositiveInteger = function(val) {
  return Number.isInteger(val) && (val > 0);
}

misc.isNegativeInteger = function(val) {
  return Number.isInteger(val) && (val < 0);
}

misc.isString = function(val) {
  return (typeof val === 'string');
}

misc.stringify = function(data) {
  return (typeof(data) === 'string') ? data : JSON.stringify(data);
}

misc.bufferify = function(data) {
  return (data instanceof Buffer) ? data : new Buffer(this.stringify(data));
}

module.exports = misc;
