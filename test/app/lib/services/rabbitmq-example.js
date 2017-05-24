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

  handler.consume(function(message, info, done) {
    console.log('==@ Received message: %s, info: %s', message, info);
    done();
  });

  var arr = [];
  for(var i=0; i<10; i++) arr.push(i);

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
    }
  }
};

module.exports = Service;
