'use strict';

var Devebot = require('devebot');
var Promise = Devebot.require('bluebird');
var amqp = require('amqplib/callback_api');
var lodash = Devebot.require('lodash');
var debug = Devebot.require('debug');
var debuglog = debug('devebot:co:rabbitmq:rabbitmqHandler');

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

  config.promisedConsumer = params.promisedConsumer;

  debuglog.isEnabled && debuglog(' - configuration object: %s', JSON.stringify(config));

  Object.defineProperty(this, 'config', {
    get: function() { return config; },
    set: function(value) {}
  });

  debuglog.isEnabled && debuglog(' - constructor end!');
};

var getConnect = function() {
  var self = this;

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

Handler.prototype.prepare = function() {
  var self = this;
  self.publisherState = self.publisherState || {};
  return getChannel.call({ config: self.config, store: self.publisherState });
}

Handler.prototype.publish = function(data) {
  var self = this;
  self.publisherState = self.publisherState || {};

  debuglog.isEnabled && debuglog('publish() an object to rabbitmq');
  return getChannel.call({ config: self.config, store: self.publisherState }).then(function(ch) {
    var dataStr = null;
    if (typeof(data) === 'string') {
      dataStr = data;
    } else {
      try {
        dataStr = JSON.stringify(data);
      } catch(err) {
        debuglog.isEnabled && debuglog('publish() invalid JSON object: %s', JSON.stringify(err));
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
  // if (!self.config.promisedConsumer) return promisedConsumer.call(self, callback);

  self.consumerState = self.consumerState || {};
  self.consumerState.count = self.consumerState.count || 0;

  debuglog.isEnabled && debuglog('publish() an object to rabbitmq');
  return getChannel.call({ config: self.config, store: self.consumerState }).then(function(ch) {
    var ch_assertExchange = Promise.promisify(ch.assertExchange, {context: ch});
    var ch_assertQueue = Promise.promisify(ch.assertQueue, {context: ch});
    var ch_bindQueue = Promise.promisify(ch.bindQueue, {context: ch});
    var ch_consume = Promise.promisify(ch.consume, {context: ch});

    var ok = ch_assertExchange(self.config.exchange, self.config.exchangeType, {
      durable: self.config.durable
    });
    ok = ok.then(function() {
      return ch_assertQueue(self.config.queue, {
        durable: self.config.durable,
        exclusive: self.config.exclusive
      });
    });
    ok = ok.then(function(qok) {
      return ch_bindQueue(qok.queue, self.config.exchange, self.config.routingKey, {}).then(function() {
        debuglog.isEnabled && debuglog(' - queue: %s has been bound', qok.queue);
        return qok.queue;
      });
    });
    ok = ok.then(function(queue) {
      return ch_consume(queue, function(msg) {
        self.consumerState.count++;
        debuglog.isEnabled && debuglog(' - received message: %s, fields: %s, amount: %s', 
          msg.content, JSON.stringify(msg.fields), self.consumerState.count);
        if (callback && typeof(callback) === 'function') {
          callback(msg.content, function done(err) {
            if (self.config.noAck === false) {
              err ? ch.nack(msg) :ch.ack(msg);
            }
            self.consumerState.count--;
          });
        } else {
          debuglog.isEnabled && debuglog(' - callback (observer) is not available');
        }
      }, {noAck: self.config.noAck});
    });
    return ok.then(function(result) {
      debuglog.isEnabled && debuglog('consumerTag: %s. To exit press CTRL+C', result.consumerTag);
      self.consumerState.consumerTag = result.consumerTag;
      return result;
    });
  });
}

var amqplib = require('amqplib');
var promisedConsumer = function(callback) {
  var self = this;

  self.consumerState = self.consumerState || {};
  self.consumerState.count = self.consumerState.count || 0;

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
          self.consumerState.count++;
          debuglog.isEnabled && debuglog(' - received message: %s, fields: %s, amount: %s', 
            msg.content, JSON.stringify(msg.fields), self.consumerState.count);
          if (callback && typeof(callback) === 'function') {
            callback(msg.content, function done(err) {
              if (self.config.noAck === false) {
                err ? ch.nack(msg) :ch.ack(msg);
              }
              self.consumerState.count--;
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

Handler.prototype.destroy = function() {
  var self = this;
  self.consumerState = self.consumerState || {};

  debuglog.isEnabled && debuglog('destroy() has been invoked: %s', self.consumerState.consumerTag);

  return Promise.resolve().then(function() {
    if (self.consumerState.consumerTag && self.consumerState.channel) {
      var ch_cancel = Promise.promisify(self.consumerState.channel.cancel, {
        context: self.consumerState.channel
      });
      return ch_cancel(self.consumerState.consumerTag).then(function(ok) {
        delete self.consumerState.consumerTag;
        debuglog.isEnabled && debuglog('destroy() - consumer is cancelled: %s', JSON.stringify(ok));
        return true;
      });
    }
    return true;
  }).then(function() {
    if (self.consumerState.channel) {
      var ch_close = Promise.promisify(self.consumerState.channel.close, {
        context: self.consumerState.channel
      });
      return ch_close().then(function(ok) {
        delete self.consumerState.channel;
        debuglog.isEnabled && debuglog('destroy() - channel is closed: %s', JSON.stringify(ok));
        return true;
      });
    }
    return true;
  }).then(function() {
    if (self.consumerState.connection) {
      var ch_close = Promise.promisify(self.consumerState.connection.close, {
        context: self.consumerState.connection
      });
      return ch_close().then(function(ok) {
        delete self.consumerState.connection;
        debuglog.isEnabled && debuglog('destroy() - connection is closed: %s', JSON.stringify(ok));
        return true;
      });
    }
    return true;
  });
}

module.exports = Handler;
