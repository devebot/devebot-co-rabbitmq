'use strict';

var events = require('events');
var util = require('util');
var Devebot = require('devebot');
var Promise = Devebot.require('bluebird');
var lodash = Devebot.require('lodash');
var debug = Devebot.require('debug');
var debuglog = debug('devebot:co:rabbitmq:example');

var Service = function(params) {
  debuglog.isEnabled && debuglog(' + constructor begin ...');

  params = params || {};

  var self = this;

  self.logger = params.loggingFactory.getLogger();

  debuglog.isEnabled && debuglog(' - NOTICE: This is a testing program');

  var handler = params.rabbitmqWrapper.open();

  handler.consume(function(error, message) {
    console.log('==@ Received message: %s', message);
  });

  handler.publish({ code: 1, msg: 'Hello world' });

  debuglog.isEnabled && debuglog(' - constructor end!');
};

Service.argumentSchema = {
  "id": "rabbitmqExample",
  "type": "object",
  "properties": {
    "sandboxName": {
      "type": "string"
    },
    "sandboxConfig": {
      "type": "object"
    },
    "profileConfig": {
      "type": "object"
    },
    "generalConfig": {
      "type": "object"
    },
    "loggingFactory": {
      "type": "object"
    },
    "rabbitmqWrapper": {
      "type": "object"
    }
  }
};

module.exports = Service;
