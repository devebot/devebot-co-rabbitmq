'use strict';

var Devebot = require('devebot');
var Promise = Devebot.require('bluebird');
var amqp = require('amqplib/callback_api');
var lodash = Devebot.require('lodash');
var debug = Devebot.require('debug');
var debugx = debug('devebot:co:rabbitmq:rabbitmqHandler');
var debug0 = debug('trace:devebot:co:rabbitmq:rabbitmqHandler');
var EventEmitter = require('events');

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

  if (typeof(params.maxSubscribers) === 'number') {
    config.maxSubscribers = params.maxSubscribers;
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

  Object.defineProperty(this, 'outlet', {
    get: function() { return getProducerState.call(self); },
    set: function(value) {}
  });

  debugx.enabled && debugx(' - constructor end!');
};

var getProducerState = function() {
  return (this.producerState = this.producerState || new EventEmitter());
}

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
        if (lodash.isFunction(self.store.emit)) {
          ['drain'].forEach(function(eventName) {
            ch.on(eventName, function(data) {
              self.store.emit(eventName, data);
            });
          });
        }
        ch.on('close', function() {
          self.store.channel = null;
        });
        debugx.enabled && debugx('getChannel() - channel is created successfully');
        return (self.store.channel = ch);
      });
    });
  }
}

var stringifyData = function(data) {
  var dataStr = null;
  if (typeof(data) === 'string') {
    dataStr = data;
  } else {
    try {
      dataStr = JSON.stringify(data);
    } catch(err) {
      debugx.enabled && debugx('invalid JSON object: %s', JSON.stringify(err));
    }
  }
  if (dataStr) {
    return Promise.resolve(dataStr);
  } else {
    return Promise.reject({
      failed: 1, successful: 0, data: data,
      message: 'Invalid JSON string/object'
    });
  }
}

var assertExchange = function(ch) {
  var self = this;
  if (self.store.exchangeAsserted) return Promise.resolve();
  var ch_assertExchange = Promise.promisify(ch.assertExchange, {context: ch});
  return ch_assertExchange(self.config.exchange, self.config.exchangeType, {
    durable: self.config.durable
  }).then(function(eok) {
    self.store.exchangeAsserted = true;
    return eok;
  });
}

var prepareExchange = function(override) {
  override = override || {};
  var self = this;
  return getChannel.call(self).then(function(ch) {
    var ok = assertExchange.call(self, ch);
    return ok.then(function(eok) {
      var _exchangeName = self.config.exchange;
      var _routingKey = override.routingKey || self.config.routingKey;
      return {exchangeName: _exchangeName, routingKey: _routingKey, channel: ch};
    });
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
      return stringifyData(data).then(function(dataStr) {
        return ch.sendToQueue(self.config.queue, new Buffer(dataStr), opts);
      }).then(function(result) {
        if (!result) {
          return Promise.reject({
            failed: 1,
            successful: 0,
            message: '[producer] stream is overflowed',
            data: data
          });
        }
        debugx.enabled && debugx('%s() has been finished successfully', self.name);
        return Promise.resolve({ failed: 0, successful: 1, data: data });
      });
    });
  });
}

Handler.prototype.ready = Handler.prototype.prepare = function() {
  var self = this;
  return getChannel.call({ config: self.config, store: getProducerState.call(self) }).then(function(ch) {
    return assertExchange.call({ config: self.config, store: self.producerState }, ch);
  });
}

Handler.prototype.produce = function(data, opts, override) {
  opts = opts || {};

  var self = this;

  debugx.enabled && debugx('produce() an object to rabbitmq');
  return prepareExchange.call({
    config: self.config,
    store: getProducerState.call(self)
  }, override).then(function(ref) {
    return stringifyData(data).then(function(dataStr) {
      return ref.channel.publish(ref.exchangeName, ref.routingKey, new Buffer(dataStr), opts);
    }).then(function(result) {
      if (!result) {
        return Promise.reject({
          failed: 1, successful: 0, data: data,
          message: '[producer] stream is overflowed'
        });
      }
      return Promise.resolve({ failed: 0, successful: 1, data: data });
    });
  });
}

Handler.prototype.enqueue = Handler.prototype.produce;
Handler.prototype.publish = Handler.prototype.produce;

Handler.prototype.checkChain = function() {
  var self = this;
  self.consumerState = self.consumerState || {};
  return getChannel.call({ config: self.config, store: self.consumerState }).then(function(ch) {
    return checkQueue.call({ config: self.config }, ch);
  });
}

Handler.prototype.purgeChain = function() {
  var self = this;
  self.consumerState = self.consumerState || {};
  return getChannel.call({ config: self.config, store: self.consumerState }).then(function(ch) {
    return assertQueue.call({ config: self.config }, ch).then(function() {
      return purgeQueue.call({ config: self.config }, ch);
    });
  });
}

Handler.prototype.checkTrash = function() {
  var self = this;
  if (!hasRecycler.call(self)) return Promise.reject(unavailableRecyclerError);
  var recyclerCfg = self.config.recycler;
  self.recyclerState = self.recyclerState || {};
  return getChannel.call({ config: recyclerCfg, store: self.recyclerState }).then(function(ch) {
    return checkQueue.call({ config: recyclerCfg }, ch);
  });
}

Handler.prototype.purgeTrash = function() {
  var self = this;
  if (!hasRecycler.call(self)) return Promise.reject(unavailableRecyclerError);
  var recyclerCfg = self.config.recycler;
  self.recyclerState = self.recyclerState || {};
  return getChannel.call({ config: recyclerCfg, store: self.recyclerState }).then(function(ch) {
    return assertQueue.call({ config: recyclerCfg }, ch).then(function() {
      return purgeQueue.call({ config: recyclerCfg }, ch);
    });
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

    var ok = assertExchange.call({ config: self.config, store: self.consumerState }, ch);

    ok = ok.then(function() {
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
      if (self.config.maxSubscribers && self.config.maxSubscribers <= qok.consumerCount) {
        var error = {
          consumerCount: qok.consumerCount,
          maxSubscribers: self.config.maxSubscribers,
          message: 'exceeding quota limits of consumers'
        }
        debugx.enabled && debugx('consume() - Error: %s', JSON.stringify(error));
        return Promise.reject(error);
      }
      var ch_consume = Promise.promisify(ch.consume, {context: ch});
      return ch_consume(qok.queue, function(msg) {
        self.consumerState.count++;
        debug0.enabled && debug0('consume() - received message: %s, fields: %s, amount: %s', 
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

Handler.prototype.process = Handler.prototype.consume;

var hasRecycler = function() {
  var recyclerCfg = this.config.recycler;
  return recyclerCfg && (recyclerCfg.enabled !== false);
}

var unavailableRecyclerError = {
  message: 'The recycler is unavailable'  
}

var discard = function(data, opts) {
  var self = this;
  if (!hasRecycler.call(self)) return Promise.reject(unavailableRecyclerError);
  var recyclerCfg = self.config.recycler;
  self.recyclerState = self.recyclerState || {};
  return sendToQueue.call({ name: 'discard', config: recyclerCfg, store: self.recyclerState }, data, opts);
}

Handler.prototype.recycle = function(callback) {
  var self = this;

  if (!hasRecycler.call(self)) return Promise.reject(unavailableRecyclerError);
  var recyclerCfg = self.config.recycler;

  self.recyclerState = self.recyclerState || {};
  self.recyclerState.count = self.recyclerState.count || 0;

  debugx.enabled && debugx('recycle() an object from rabbitmq');
  return getChannel.call({ config: recyclerCfg, store: self.recyclerState }).then(function(ch) {

    if (recyclerCfg.prefetch && recyclerCfg.prefetch >= 0) {
      debugx.enabled && debugx('recycle() - set channel prefetch: %s', recyclerCfg.prefetch);
      ch.prefetch(recyclerCfg.prefetch, true);
    }

    var ok = assertQueue.call({ config: recyclerCfg }, ch).then(function(qok) {
      debugx.enabled && debugx('recycle() - queue info: %s', JSON.stringify(qok));
      if (recyclerCfg.maxSubscribers && recyclerCfg.maxSubscribers <= qok.consumerCount) {
        var error = {
          consumerCount: qok.consumerCount,
          maxSubscribers: recyclerCfg.maxSubscribers,
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
            if (recyclerCfg.noAck === false) {
              err ? ch.nack(msg) :ch.ack(msg);
            }
            self.recyclerState.count--;
          });
        } else {
          debugx.enabled && debugx('recycle() - callback (observer) is not available');
        }
      }, {noAck: recyclerCfg.noAck});
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
      }).delay(200);
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
