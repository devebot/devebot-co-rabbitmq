'use strict';

var events = require('events');
var util = require('util');

events.EventEmitter.defaultMaxListeners = 100;

var Devebot = require('devebot');
var Promise = Devebot.require('bluebird');
var lodash = Devebot.require('lodash');

var debug = Devebot.require('debug');
var debuglog = debug('devebotBridgeNodemailer:test:bdd:world');

var app = require('../../../app/index.js');

var World = function World(callback) {
  this.app = app;

  var configsandbox = this.app.config.sandbox.context[process.env.NODE_DEVEBOT_SANDBOX];

  var app_conf = configsandbox.application;
  debuglog.isEnabled && debuglog(' - Application Config: %s', JSON.stringify(app_conf));
};

module.exports.World = World;
