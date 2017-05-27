'use strict';

var Devebot = require('devebot');
var Promise = Devebot.require('bluebird');
var amqp = require('amqplib/callback_api');
var lodash = Devebot.require('lodash');
var debug = Devebot.require('debug');
var debugx = debug('devebot:co:rabbitmq:rabbitmqHandler');

var Handler = function(params) {
  debugx.enabled && debugx(' + constructor begin ...');

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
  config.prefetch = (typeof(params.prefetch) === 'number') ? params.prefetch : undefined;
  config.connectIsCached = typeof(params.connectIsCached) === 'boolean' ? params.connectIsCached : true;
  config.channelIsCached = typeof(params.channelIsCached) === 'boolean' ? params.channelIsCached : true;

  if (typeof(params.maxConsumers) === 'number') {
    config.maxConsumers = params.maxConsumers;
  }

  if (params.recycler && typeof(params.recycler) === 'object') {
    config.recycler = lodash.merge({
      host: params.host,
      redeliveredCountName: 'x-redelivered-count',
      redeliveredLimit: 5,
    }, params.recycler);
    if (lodash.isEmpty(config.recycler.queue)) {
      config.recycler.queue = config.queue + '-trashed';
    }
  }

  debugx.enabled && debugx(' - configuration object: %s', JSON.stringify(config));

  Object.defineProperty(this, 'config', {
    get: function() { return config; },
    set: function(value) {}
  });

  debugx.enabled && debugx(' - constructor end!');
};

var getConnect = function() {
  var self = this;

  self.store.connectionCount = self.store.connectionCount || 0;
  debugx.enabled && debugx('getConnect() - connection amount: %s', self.store.connectionCount);

  if (self.config.connectIsCached !== false && self.store.connection) {
    debugx.enabled && debugx('getConnect() - connection has been available');
    return Promise.resolve(self.store.connection);
  } else {
    debugx.enabled && debugx('getConnect() - make a new connection');
    var amqp_connect = Promise.promisify(amqp.connect, {context: amqp});
    return amqp_connect(self.config.host, {}).then(function(conn) {
      self.store.connectionCount += 1;
      conn.on('close', function() {
        self.store.connection = null;
        self.store.connectionCount--;
      });
      debugx.enabled && debugx('getConnect() - connection is created successfully');
      return (self.store.connection = conn);
    });
  }
}

var getChannel = function() {
  var self = this;

  if (self.config.channelIsCached !== false && self.store.channel) {
    debugx.enabled && debugx('getChannel() - channel has been available');
    return Promise.resolve(self.store.channel);
  } else {
    debugx.enabled && debugx('getChannel() - make a new channel');
    return getConnect.call(self).then(function(conn) {
      debugx.enabled && debugx('getChannel() - connection has already');
      var createChannel = Promise.promisify(conn.createChannel, {context: conn});
      return createChannel().then(function(ch) {
        ch.on('close', function() {
          self.store.channel = null;
        });
        debugx.enabled && debugx('getChannel() - channel is created successfully');
        return (self.store.channel = ch);
      });
    });
  }
}

var assertExchange = function(ch) {
  var self = this;
  var ch_assertExchange = Promise.promisify(ch.assertExchange, {context: ch});
  return ch_assertExchange(self.config.exchange, self.config.exchangeType, {
    durable: self.config.durable
  });
}

var assertQueue = function(ch) {
  var self = this;
  return Promise.promisify(ch.assertQueue, {context: ch})(self.config.queue, {
    durable: self.config.durable,
    exclusive: self.config.exclusive
  }).then(function(qok) {
    self.config.queue = self.config.queue || qok.queue;
    return qok;
  });
}

var checkQueue = function(ch) {
  var self = this;
  return Promise.promisify(ch.checkQueue, {context: ch})(self.config.queue);
}

var purgeQueue = function(ch) {
  var self = this;
  return Promise.promisify(ch.purgeQueue, {context: ch})(self.config.queue);
}

var sendToQueue = function(data, opts) {
  opts = opts || {};
  var self = this;

  debugx.enabled && debugx('%s() an object to rabbitmq queue', self.name);
  return getChannel.call({ config: self.config, store: self.store }).then(function(ch) {
    return assertQueue.call({ config: self.config }, ch).then(function() {
      var dataStr = null;
      if (typeof(data) === 'string') {
        dataStr = data;
      } else {
        try {
          dataStr = JSON.stringify(data);
        } catch(err) {
          debugx.enabled && debugx('%s() invalid JSON object: %s', JSON.stringify(err), self.name);
        }
      }
      if (!dataStr) {
        return Promise.reject({
          failed: 1,
          successful: 0,
          message: 'Invalid JSON string/object',
          data: data
        });
      }
      var result = ch.sendToQueue(self.config.queue, new Buffer(dataStr), opts);
      if (!result) {
        return Promise.reject({
          failed: 1,
          successful: 0,
          message: 'Publisher stream is overflowed',
          data: data
        });
      }
      debugx.enabled && debugx('%s() has been finished successfully', self.name);
      return Promise.resolve({ failed: 0, successful: 1, data: data });
    });
  });
}

Handler.prototype.ready = Handler.prototype.prepare = function() {
  var self = this;
  self.publisherState = self.publisherState || {};
  return getChannel.call({ config: self.config, store: self.publisherState }).then(function(ch) {
    return assertExchange.call({ config: self.config }, ch);
  });
}

Handler.prototype.publish = function(data, opts) {
  opts = opts || {};
  var self = this;
  self.publisherState = self.publisherState || {};

  debugx.enabled && debugx('publish() an object to rabbitmq');
  return getChannel.call({ config: self.config, store: self.publisherState }).then(function(ch) {
    var dataStr = null;
    if (typeof(data) === 'string') {
      dataStr = data;
    } else {
      try {
        dataStr = JSON.stringify(data);
      } catch(err) {
        debugx.enabled && debugx('publish() invalid JSON object: %s', JSON.stringify(err));
      }
    }
    if (!dataStr) {
      return Promise.reject({
        failed: 1,
        successful: 0,
        message: 'Invalid JSON string/object',
        data: data
      });
    }
    var result = ch.publish(self.config.exchange, self.config.routingKey, new Buffer(dataStr), opts);
    if (!result) {
      return Promise.reject({
        failed: 1,
        successful: 0,
        message: 'Publisher stream is overflowed',
        data: data
      });
    }
    return Promise.resolve({ failed: 0, successful: 1, data: data });
  });
}

Handler.prototype.countChainMessages = function() {
  var self = this;
  self.consumerState = self.consumerState || {};
  return getChannel.call({ config: self.config, store: self.consumerState }).then(function(ch) {
    return checkQueue.call({ config: self.config }, ch);
  }).then(function(qok) {
    return qok.messageCount;
  });
}

Handler.prototype.purgeChain = function() {
  var self = this;
  self.consumerState = self.consumerState || {};
  return getChannel.call({ config: self.config, store: self.consumerState }).then(function(ch) {
    return purgeQueue.call({ config: self.config }, ch);
  });
}

Handler.prototype.countTrashMessages = function() {
  var self = this;
  if (!hasRecycler.call(self)) return Promise.reject(unavailableRecyclerError);
  var recyclerCfg = self.config.recycler;
  self.recyclerState = self.recyclerState || {};
  return getChannel.call({ config: recyclerCfg, store: self.recyclerState }).then(function(ch) {
    return checkQueue.call({ config: recyclerCfg }, ch);
  }).then(function(qok) {
    return qok.messageCount;
  });
}

Handler.prototype.purgeTrash = function() {
  var self = this;
  if (!hasRecycler.call(self)) return Promise.reject(unavailableRecyclerError);
  var recyclerCfg = self.config.recycler;
  self.recyclerState = self.recyclerState || {};
  return getChannel.call({ config: recyclerCfg, store: self.recyclerState }).then(function(ch) {
    return purgeQueue.call({ config: recyclerCfg }, ch);
  });
}

var requeue = function(data, opts) {
  var self = this;
  self.consumerState = self.consumerState || {};
  return sendToQueue.call({ name: 'requeue', config: self.config, store: self.consumerState }, data, opts);
}

Handler.prototype.consume = function(callback) {
  var self = this;

  self.consumerState = self.consumerState || {};
  self.consumerState.count = self.consumerState.count || 0;

  debugx.enabled && debugx('consume() an object from rabbitmq');
  return getChannel.call({ config: self.config, store: self.consumerState }).then(function(ch) {

    if (self.config.prefetch && self.config.prefetch >= 0) {
      debugx.enabled && debugx('consume() - set channel prefetch: %s', self.config.prefetch);
      ch.prefetch(self.config.prefetch, true);
    }

    var ok = assertExchange.call({ config: self.config }, ch).then(function() {
      return assertQueue.call({ config: self.config }, ch);
    });

    ok = ok.then(function(qok) {
      var ch_bindQueue = Promise.promisify(ch.bindQueue, {context: ch});
      return ch_bindQueue(qok.queue, self.config.exchange, self.config.routingKey, {}).then(function() {
        debugx.enabled && debugx('consume() - queue: %s has been bound', qok.queue);
        return qok;
      });
    });

    ok = ok.then(function(qok) {
      debugx.enabled && debugx('consume() - queue info: %s', JSON.stringify(qok));
      if (self.config.maxConsumers && self.config.maxConsumers <= qok.consumerCount) {
        var error = {
          consumerCount: qok.consumerCount,
          maxConsumers: self.config.maxConsumers,
          message: 'exceeding quota limits of consumers'
        }
        debugx.enabled && debugx('consume() - Error: %s', JSON.stringify(error));
        return Promise.reject(error);
      }
      var ch_consume = Promise.promisify(ch.consume, {context: ch});
      return ch_consume(qok.queue, function(msg) {
        self.consumerState.count++;
        debugx.enabled && debugx('consume() - received message: %s, fields: %s, amount: %s', 
          msg.content, JSON.stringify(msg.fields), self.consumerState.count);
        if (callback && typeof(callback) === 'function') {
          callback(msg.content, {
            fields: msg.fields,
            properties: msg.properties
          }, function done(err) {
            if (self.config.noAck === false) {
              if (hasRecycler.call(self)) {
                if (err) {
                  var recyclerCfg = self.config.recycler;
                  var headers = msg.properties && msg.properties.headers || {};
                  headers[recyclerCfg.redeliveredCountName] = (headers[recyclerCfg.redeliveredCountName] || 0) + 1;
                  if (headers[recyclerCfg.redeliveredCountName] <= recyclerCfg.redeliveredLimit) {
                    requeue.call(self, msg.content.toString(), {headers: headers});
                  } else {
                    discard.call(self, msg.content.toString(), {headers: headers});
                  }
                  ch.ack(msg);
                } else {
                  ch.ack(msg);
                }
              } else {
                err ? ch.nack(msg) :ch.ack(msg);
              }
            }
            self.consumerState.count--;
          });
        } else {
          debugx.enabled && debugx('consume() - callback (observer) is not available');
        }
      }, {noAck: self.config.noAck});
    });

    return ok.then(function(result) {
      debugx.enabled && debugx('consume() - consumerTag: %s. CTRL+C to exit', result.consumerTag);
      self.consumerState.consumerTag = result.consumerTag;
      return result;
    });
  });
}

var hasRecycler = function() {
  var recyclerConfig = this.config.recycler;
  return recyclerConfig && (recyclerConfig.enabled !== false);
}

var unavailableRecyclerError = {
  message: 'The recycler is unavailable'  
}

var discard = function(data, opts) {
  var self = this;
  if (!hasRecycler.call(self)) return Promise.reject(unavailableRecyclerError);
  var recyclerConfig = self.config.recycler;
  self.recyclerState = self.recyclerState || {};
  return sendToQueue.call({ name: 'discard', config: recyclerConfig, store: self.recyclerState }, data, opts);
}

Handler.prototype.recycle = function(callback) {
  var self = this;

  if (!hasRecycler.call(self)) return Promise.reject(unavailableRecyclerError);
  var recyclerConfig = self.config.recycler;

  self.recyclerState = self.recyclerState || {};
  self.recyclerState.count = self.recyclerState.count || 0;

  debugx.enabled && debugx('recycle() an object from rabbitmq');
  return getChannel.call({ config: recyclerConfig, store: self.recyclerState }).then(function(ch) {

    if (recyclerConfig.prefetch && recyclerConfig.prefetch >= 0) {
      debugx.enabled && debugx('recycle() - set channel prefetch: %s', recyclerConfig.prefetch);
      ch.prefetch(recyclerConfig.prefetch, true);
    }

    var ok = assertQueue.call({ config: recyclerConfig }, ch).then(function(qok) {
      debugx.enabled && debugx('recycle() - queue info: %s', JSON.stringify(qok));
      if (recyclerConfig.maxConsumers && recyclerConfig.maxConsumers <= qok.consumerCount) {
        var error = {
          consumerCount: qok.consumerCount,
          maxConsumers: recyclerConfig.maxConsumers,
          message: 'exceeding quota limits of consumers'
        }
        debugx.enabled && debugx('recycle() - Error: %s', JSON.stringify(error));
        return Promise.reject(error);
      }
      return Promise.promisify(ch.consume, {context: ch})(qok.queue, function(msg) {
        self.recyclerState.count++;
        debugx.enabled && debugx('recycle() - received message: %s, fields: %s, amount: %s', 
          msg.content, JSON.stringify(msg.fields), self.recyclerState.count);
        if (callback && typeof(callback) === 'function') {
          callback(msg.content, {
            fields: msg.fields,
            properties: msg.properties
          }, function done(err) {
            if (recyclerConfig.noAck === false) {
              err ? ch.nack(msg) :ch.ack(msg);
            }
            self.recyclerState.count--;
          });
        } else {
          debugx.enabled && debugx('recycle() - callback (observer) is not available');
        }
      }, {noAck: recyclerConfig.noAck});
    });

    return ok.then(function(result) {
      debugx.enabled && debugx('recycle() - consumerTag: %s. CTRL+C to exit', result.consumerTag);
      self.recyclerState.consumerTag = result.consumerTag;
      return result;
    });
  });
}

Handler.prototype.destroy = function() {
  var self = this;
  self.consumerState = self.consumerState || {};
  self.recyclerState = self.recyclerState || {};
  return Promise.resolve().then(function() {
    return stopListener.call({store: self.consumerState});
  }).then(function() {
    return stopListener.call({store: self.recyclerState});
  });
}

var stopListener = function() {
  var self = this;
  return Promise.resolve().then(function() {
    if (self.store.consumerTag && self.store.channel) {
      debugx.enabled && debugx('destroy() has been invoked: %s', self.store.consumerTag);
      var ch_cancel = Promise.promisify(self.store.channel.cancel, {
        context: self.store.channel
      });
      return ch_cancel(self.store.consumerTag).then(function(ok) {
        delete self.store.consumerTag;
        debugx.enabled && debugx('destroy() - consumer is cancelled: %s', JSON.stringify(ok));
        return true;
      }).delay(100);
    }
    return true;
  }).then(function() {
    if (self.store.channel) {
      var ch_close = Promise.promisify(self.store.channel.close, {
        context: self.store.channel
      });
      return ch_close().then(function(ok) {
        delete self.store.channel;
        debugx.enabled && debugx('destroy() - channel is closed: %s', JSON.stringify(ok));
        return true;
      });
    }
    return true;
  }).then(function() {
    if (self.store.connection) {
      var ch_close = Promise.promisify(self.store.connection.close, {
        context: self.store.connection
      });
      return ch_close().then(function(ok) {
        delete self.store.connection;
        debugx.enabled && debugx('destroy() - connection is closed: %s', JSON.stringify(ok));
        return true;
      });
    }
    return true;
  });
}

module.exports = Handler;
