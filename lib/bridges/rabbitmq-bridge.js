'use strict';

var events = require('events');
var util = require('util');

var Devebot = require('devebot');
var Promise = Devebot.require('bluebird');
var lodash = Devebot.require('lodash');
var debug = Devebot.require('debug');
var debuglog = debug('devebot:co:rabbitmq:rabbitmqBridge');

var noop = function() {};

var Service = function(params) {
  debuglog.isEnabled && debuglog(' + constructor start ...');

  params = params || {};

  var self = this;

  self.logger = self.logger || params.logger || { trace: noop, info: noop, debug: noop, warn: noop, error: noop };

  self.getServiceInfo = function() {
    return {};
  };

  self.getServiceHelp = function() {
    return {};
  };

  debuglog.isEnabled && debuglog(' - constructor has finished');
};

Service.argumentSchema = {
  "id": "rabbitmqBridge",
  "type": "object"
};

module.exports = Service;
