'use strict';

var events = require('events');
var util = require('util');
var Devebot = require('devebot');
var Promise = Devebot.require('bluebird');
var lodash = Devebot.require('lodash');
var debugx = Devebot.require('debug')('devebot:co:rabbitmq:example');

var Service = function(params) {
  debugx.enabled && debugx(' + constructor begin ...');

  params = params || {};

  var self = this;
  var logger = params.loggingFactory.getLogger();
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

  handler.ready().then(function() {
    arr.forEach(function(count) {
      handler.produce({ code: count, msg: 'Hello world (forEach)' }).then(function(result) {
        console.log('produce() - result: %s', JSON.stringify(result));
        return result;
      });
    })
  });

  setTimeout(function() {
    Promise.mapSeries(arr, function(count) {
      return handler.produce({ code: count, msg: 'Hello world (mapSeries)' }).then(function(result) {
        console.log('produce() - result: %s', JSON.stringify(result));
        return result;
      });
    }).then(function() {
      console.log('=============== Done ==================');
    });
  }, 7000);

  debugx.enabled && debugx(' - constructor end!');
};

Service.argumentSchema = {
  "id": "rabbitmqExample",
  "type": "object",
  "properties": {
    "rabbitmqWrapper": {
      "type": "object"
    },
    "rabbitmqExporter": {
      "type": "object"
    }
  }
};

module.exports = Service;
