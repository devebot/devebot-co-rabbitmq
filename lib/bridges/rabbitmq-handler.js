'use strict';

var Devebot = require('devebot');
var Promise = Devebot.require('bluebird');
var lodash = Devebot.require('lodash');
var debug = Devebot.require('debug');
var debuglog = debug('devebot:co:rabbitmq:rabbitmqHandler');

var amqplib = require('amqplib');

var Handler = function(params) {
  debuglog.isEnabled && debuglog(' + constructor begin ...');

  var self = this;

  var config = {};
  config.host = params.host;
  config.exchange = params.exchange;
  config.exchangeType = params.exchangeType || 'fanout';
  config.routingKey = params.routingKey || '';
  config.queue = params.queue || '';
  config.durable = (typeof(params.durable) === 'boolean') ? params.durable : true;
  config.exclusive = (typeof(params.exclusive) === 'boolean') ? params.exclusive : true;
  config.noAck = (typeof(params.noAck) === 'boolean') ? params.noAck : true;

  debuglog.isEnabled && debuglog(' - configuration object: %s', JSON.stringify(config));

  Object.defineProperty(this, 'config', {
    get: function() { return config; },
    set: function(value) {}
  });

  debuglog.isEnabled && debuglog(' - constructor end!');
};

Handler.prototype.publish = function(data) {
  var self = this;
  debuglog.isEnabled && debuglog(' - publish() an object to rabbitmq');
  return amqplib.connect(self.config.host).then(function(conn) {
    return conn.createChannel().then(function(ch) {
      var ok = ch.assertExchange(self.config.exchange, self.config.exchangeType, {durable: self.config.durable});
      return ok.then(function() {
        ch.publish(self.config.exchange, self.config.routingKey, new Buffer(JSON.stringify(data)));
        return ch.close();
      });
    }).finally(function() {
      conn.close();
    });
  });
}

Handler.prototype.consume = function(callback) {
  var self = this;
  debuglog.isEnabled && debuglog(' - consume() a message from rabbitmq');
  return amqplib.connect(self.config.host).then(function(conn) {
    return conn.createChannel().then(function(ch) {
      var ok = ch.assertExchange(self.config.exchange, self.config.exchangeType, {durable: self.config.durable});
      ok = ok.then(function() {
        return ch.assertQueue(self.config.queue, {exclusive: self.config.exclusive, durable: self.config.durable});
      });
      ok = ok.then(function(qok) {
        return ch.bindQueue(qok.queue, self.config.exchange, self.config.routingKey).then(function() {
          debuglog.isEnabled && debuglog(' - queue: %s has been bound', qok.queue);
          return qok.queue;
        });
      });
      ok = ok.then(function(queue) {
        return ch.consume(queue, function(msg) {
          debuglog.isEnabled && debuglog(' - received message: %s, fields: %s', msg.content, JSON.stringify(msg.fields));
          if (callback && typeof(callback) === 'function') {
            callback(null, msg.content);
          } else {
            debuglog.isEnabled && debuglog(' - callback (observer) is not available');
          }
        }, {noAck: self.config.noAck});
      });
      return ok.then(function() {
        debuglog.isEnabled && debuglog('[*] Waiting for logs. To exit press CTRL+C');
        return {};
      });
    });
  });
}

module.exports = Handler;
