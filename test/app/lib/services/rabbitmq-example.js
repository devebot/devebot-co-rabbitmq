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

  var exporter = params.rabbitmqExporter.open();

  var handler = params.rabbitmqWrapper.open();

  var count = 0;
  handler.consume(function(message, info, finish) {
    console.log('==@ Received message: %s, info: %s', message, JSON.stringify(info));
    message = JSON.parse(message);
    if ([11, 21, 31, 41, 51, 61, 71, 81, 91, 99].indexOf(message.code) < 0) {
      finish();
    } else {
      finish('error');
    }
  });

  var arr = [];
  for(var i=0; i<100; i++) arr.push(i);

  handler.prepare().then(function() {
    arr.forEach(function(count) {
      handler.publish({ code: count, msg: 'Hello world (forEach)' }).then(function(result) {
        console.log('publish() - result: %s', JSON.stringify(result));
        return result;
      });
    })
  });

  setTimeout(function() {
    Promise.mapSeries(arr, function(count) {
      return handler.publish({ code: count, msg: 'Hello world (mapSeries)' }).then(function(result) {
        console.log('publish() - result: %s', JSON.stringify(result));
        return result;
      });
    }).then(function() {
      console.log('=============== Done ==================');
    });
  }, 7000);

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
    },
    "rabbitmqExporter": {
      "type": "object"
    }
  }
};

module.exports = Service;
