'use strict';

var Devebot = require('devebot');
var Promise = Devebot.require('bluebird');
var lodash = Devebot.require('lodash');
var debugx = Devebot.require('debug')('devebot:co:rabbitmq:bridge');
var Handler = require('./handler');

var noop = function() {};

var Service = function(params) {
  debugx.enabled && debugx(' + constructor start ...');

  params = params || {};

  var handler = null;

  this.open = function(opts) {
    debugx.enabled && debugx(' - open a channel to %s/%s', params.amqplib.host, params.amqplib.exchange);
    return (handler = handler || new Handler(lodash.assign({logger: params.logger}, params.amqplib, opts || {})));
  }

  debugx.enabled && debugx(' - constructor end!');
};

module.exports = Service;
