'use strict';

var Devebot = require('devebot');
var Promise = Devebot.require('bluebird');
var lodash = Devebot.require('lodash');
var debug = Devebot.require('debug');
var debuglog = debug('devebot:co:rabbitmq:rabbitmqHandler');

var amqp = require('amqplib/callback_api');
var amqplib = require('amqplib');

var Handler = function(params) {
  debuglog.isEnabled && debuglog(' + constructor begin ...');

  var self = this;
  self.logger = self.logger || params.logger;

  var config = {};
  config.host = params.host;
  config.exchange = params.exchange;
  config.exchangeType = params.exchangeType || 'fanout';
  config.routingKey = params.routingKey || '';
  config.queue = params.queue || '';
  config.durable = (typeof(params.durable) === 'boolean') ? params.durable : true;
  config.exclusive = (typeof(params.exclusive) === 'boolean') ? params.exclusive : false;
  config.noAck = (typeof(params.noAck) === 'boolean') ? params.noAck : false;
  config.connectIsCached = typeof(params.connectIsCached) === 'boolean' ? params.connectIsCached : true;
  config.channelIsCached = typeof(params.channelIsCached) === 'boolean' ? params.channelIsCached : true;

  debuglog.isEnabled && debuglog(' - configuration object: %s', JSON.stringify(config));

  Object.defineProperty(this, 'config', {
    get: function() { return config; },
    set: function(value) {}
  });

  debuglog.isEnabled && debuglog(' - constructor end!');
};

var getConnect = function() {
  var self = this;
  self.store = self.store || {};

  self.store.connectionCount = self.store.connectionCount || 0;
  debuglog.isEnabled && debuglog('getConnect() - connection amount: %s', self.store.connectionCount);

  if (self.config.connectIsCached && self.store.connection) {
    debuglog.isEnabled && debuglog('getConnect() - connection has been available');
    return Promise.resolve(self.store.connection);
  } else {
    debuglog.isEnabled && debuglog('getConnect() - make a new connection');
    return new Promise(function(onResolved, onRejected) {
      amqp.connect(self.config.host, function(err, conn) {
        if (err) return onRejected(err);
        self.store.connectionCount += 1;
        conn.on('close', function() {
          self.store.connectionCount--;
        });
        debuglog.isEnabled && debuglog('getConnect() - connection is created successfully');
        onResolved(self.store.connection = conn);
      });
    });
  }
}

var getChannel = function() {
  var self = this;
  self.store = self.store || {};

  if (self.config.channelIsCached && self.store.channel) {
    debuglog.isEnabled && debuglog('getChannel() - channel has been available');
    return Promise.resolve(self.store.channel);
  } else {
    debuglog.isEnabled && debuglog('getChannel() - make a new channel');
    return getConnect.call(self).then(function(conn) {
      debuglog.isEnabled && debuglog('getChannel() - connection has already');
      var createChannel = Promise.promisify(conn.createChannel, {context: conn});
      return createChannel().then(function(ch) {
        return (self.store.channel = ch);
      });
    });
  }
}

Handler.prototype.ready = function() {
  return getChannel.call(this);
}

Handler.prototype.publish = function(data) {
  var self = this;
  debuglog.isEnabled && debuglog('publish() an object to rabbitmq');
  return getChannel.call(self).then(function(ch) {
    var dataStr = null;
    if (typeof(data) === 'string') {
      dataStr = data;
    } else {
      try {
        dataStr = JSON.stringify(data);
      } catch(err) {
        debuglog.isEnabled && debuglog('publish() invalid JSON object, err: %s', JSON.stringify(err));
      }
    }
    var result = false;
    if (dataStr) {
      result = ch.publish(self.config.exchange, self.config.routingKey, new Buffer(dataStr));
    }
    return result ? Promise.resolve() : Promise.reject();
  });
}

Handler.prototype.consume = function(callback) {
  var self = this;

  self.store = self.store || {};
  self.store.consumerCount = self.store.consumerCount || 0;

  debuglog.isEnabled && debuglog(' - consume() a message from rabbitmq');
  return amqplib.connect(self.config.host).then(function(conn) {
    return conn.createChannel().then(function(ch) {
      var ok = ch.assertExchange(self.config.exchange, self.config.exchangeType, {
        durable: self.config.durable
      });
      ok = ok.then(function() {
        return ch.assertQueue(self.config.queue, {
          durable: self.config.durable,
          exclusive: self.config.exclusive
        });
      });
      ok = ok.then(function(qok) {
        return ch.bindQueue(qok.queue, self.config.exchange, self.config.routingKey).then(function() {
          debuglog.isEnabled && debuglog(' - queue: %s has been bound', qok.queue);
          return qok.queue;
        });
      });
      ok = ok.then(function(queue) {
        return ch.consume(queue, function(msg) {
          self.store.consumerCount++;
          debuglog.isEnabled && debuglog(' - received message: %s, fields: %s, amount: %s', 
            msg.content, JSON.stringify(msg.fields), self.store.consumerCount);
          if (callback && typeof(callback) === 'function') {
            callback(msg.content, function done(err) {
              !err && !self.config.noAck && ch.ack(msg);
              self.store.consumerCount--;
            });
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
