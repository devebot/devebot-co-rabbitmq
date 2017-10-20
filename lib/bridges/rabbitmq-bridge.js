'use strict';

var Devebot = require('devebot');
var Promise = Devebot.require('bluebird');
var lodash = Devebot.require('lodash');
var debug = Devebot.require('debug');
var debuglog = debug('devebot:co:rabbitmq:rabbitmqBridge');

var Handler = require('./rabbitmq-handler');

var noop = function() {};

var Service = function(params) {
  debuglog.isEnabled && debuglog(' + constructor start ...');

  params = params || {};

  var self = this;

  self.logger = self.logger || params.logger || { trace: noop, info: noop, debug: noop, warn: noop, error: noop };

  var handler = null;

  self.open = function(opts) {
    debuglog.isEnabled && debuglog(' - open a channel to %s/%s', params.amqplib.host, params.amqplib.exchange);
    return (handler = handler || new Handler(lodash.assign({logger: params.logger}, params.amqplib, opts || {})));
  }

  debuglog.isEnabled && debuglog(' - constructor end!');
};

Service.argumentSchema = {
  "id": "rabbitmqBridge",
  "type": "object"
};

module.exports = Service;
