'use strict';

var Devebot = require('devebot');
var Promise = Devebot.require('bluebird');
var amqp = require('amqplib/callback_api');
var locks = require('locks');
var lodash = Devebot.require('lodash');
var debug = Devebot.require('debug');
var debugx = debug('devebot:co:rabbitmq:rabbitmqHandler');
var debug0 = debug('trace:devebot:co:rabbitmq:rabbitmqHandler');
var debug2 = debug('devebot:co:rabbitmq:rabbitmqHandler:extract');
var Readable = require('stream').Readable;
var EventEmitter = require('events');

var Handler = function(params) {
  debugx.enabled && debugx(' + constructor begin ...');

  var self = this;
  self.logger = self.logger || params.logger;

  var config = {};
  config.uri = params.uri || params.host;
  config.exchangeName = params.exchangeName || params.exchange;
  config.exchangeType = params.exchangeType || 'fanout';
  if (typeof(params.exchangeMutex) === 'boolean') {
    config.exchangeMutex = params.exchangeMutex;
  }
  config.durable = (typeof(params.durable) === 'boolean') ? params.durable : true;
  config.autoDelete = (typeof(params.autoDelete) === 'boolean') ? params.autoDelete : false;
  if (typeof(params.alternateExchange) === 'string') {
    config.alternateExchange = params.alternateExchange;
  }
  config.routingKey = params.routingKey || '';

  config.consumer = { uri: config.uri };
  if (params.consumer && typeof(params.consumer) === 'object') {
    lodash.merge(config.consumer, params.consumer);
  }

  if (typeof(config.consumer.queueName) !== 'string') {
    config.consumer.queueName = params.queueName || params.queue || '';
  }
  if (typeof(config.consumer.durable) !== 'boolean') {
    config.consumer.durable = (typeof(params.durable) === 'boolean') ? params.durable : true;
  }
  if (typeof(config.consumer.exclusive) !== 'boolean') {
    config.consumer.exclusive = (typeof(params.exclusive) === 'boolean') ? params.exclusive : false;
  }
  if (typeof(config.consumer.noAck) !== 'boolean') {
    config.consumer.noAck = (typeof(params.noAck) === 'boolean') ? params.noAck : false;
  }
  if (typeof(config.consumer.prefetch) !== 'number') {
    config.consumer.prefetch = (typeof(params.prefetch) === 'number') ? params.prefetch : undefined;
  }
  if (typeof(config.consumer.connectIsCached) !== 'boolean') {
    config.consumer.connectIsCached = typeof(params.connectIsCached) === 'boolean' ? params.connectIsCached : true;
  }
  if (typeof(config.consumer.connectIsCached) !== 'boolean') {
    config.consumer.channelIsCached = typeof(params.channelIsCached) === 'boolean' ? params.channelIsCached : true;
  }
  if (typeof(config.consumer.maxSubscribers) !== 'number') {
    config.consumer.maxSubscribers = params.maxSubscribers;
  }

  if (params.recycler && typeof(params.recycler) === 'object') {
    config.recycler = {
      uri: config.uri,
      redeliveredCountName: 'x-redelivered-count',
      redeliveredLimit: 5,
    }
    lodash.merge(config.recycler, params.recycler);
    if (lodash.isEmpty(config.recycler.queueName)) {
      config.recycler.queueName = config.consumer.queueName + '-trashed';
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
    return amqp_connect(self.config.uri, {}).then(function(conn) {
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
        // if (lodash.isFunction(self.store.emit)) {
        //   ['drain'].forEach(function(eventName) {
        //     ch.on(eventName, function(data) {
        //       self.store.emit(eventName, data);
        //     });
        //   });
        // }
        ch.setMaxListeners(10000);
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
  return (typeof(data) === 'string') ? data : JSON.stringify(data);
}

var assertExchange = function(ch) {
  var self = this;
  if (self.store.exchangeAsserted) return Promise.resolve();
  var ch_assertExchange = Promise.promisify(ch.assertExchange, {context: ch});
  return ch_assertExchange(self.config.exchangeName, self.config.exchangeType, {
    durable: self.config.durable,
    autoDelete: self.config.autoDelete
  }).then(function(eok) {
    self.store.exchangeAsserted = true;
    return eok;
  });
}

var retrieveExchange = function(override) {
  var self = this;
  override = override || {};
  return getChannel.call(self).then(function(ch) {
    var ok = assertExchange.call(self, ch);
    return ok.then(function(eok) {
      return {
        channel: ch,
        exchangeName: self.config.exchangeName,
        routingKey: override.routingKey || self.config.routingKey
      };
    });
  });
}

var assertQueue = function(ch) {
  var self = this;
  return Promise.promisify(ch.assertQueue, {context: ch})(self.config.queueName, {
    durable: self.config.durable,
    exclusive: self.config.exclusive
  }).then(function(qok) {
    self.config.queueName = self.config.queueName || qok.queue;
    return qok;
  });
}

var checkQueue = function(ch) {
  var self = this;
  return Promise.promisify(ch.checkQueue, {context: ch})(self.config.queueName);
}

var purgeQueue = function(ch) {
  var self = this;
  return Promise.promisify(ch.purgeQueue, {context: ch})(self.config.queueName);
}

var sendToQueue = function(data, opts) {
  var self = this;
  opts = opts || {};
  var sendTo = function(ch) {
    self.config.sendable = ch.sendToQueue(self.config.queueName, new Buffer(stringifyData(data)), opts);
  }
  debugx.enabled && debugx('%s() an object to rabbitmq queue', self.name);
  return getChannel.call({ config: self.config, store: self.store }).then(function(ch) {
    return assertQueue.call({ config: self.config }, ch).then(function() {
      if (self.config.sendable !== false) {
        sendTo(ch);
        debugx.enabled && debugx('%s() channel is writable, msg has been sent', self.name);
      } else {
        ch.once('drain', function() {
          sendTo(ch);
          debugx.enabled && debugx('%s() channel is drained, flushed', self.name);
        });
        debugx.enabled && debugx('%s() channel is overflowed, waiting', self.name);
      }
      return self.config.sendable;
    });
  });
}

Handler.prototype.ready = Handler.prototype.prepare = function() {
  var self = this;
  return getChannel.call({ config: self.config, store: getProducerState.call(self) }).then(function(ch) {
    return assertExchange.call({ config: self.config, store: getProducerState.call(self) }, ch);
  });
}

var getProducerState = function() {
  this.producerState = this.producerState || new EventEmitter();
  if (this.config.exchangeMutex) {
    this.producerState.fence = this.producerState.fence || locks.createCondVariable(true);
  }
  return (this.producerState);
}

var lockProducer = function(override) {
  var self = this;
  var producerState = getProducerState.call(self);
  return retrieveExchange.call({ config: self.config, store: producerState }, override).then(function(ref) {
    if (producerState.fence) {
      return new Promise(function(resolved, rejected) {
        producerState.fence.wait(
          function conditionTest(value) {
            return value === true;
          },
          function whenConditionMet() {
            resolved(ref);
          }
        );
      });
    }
    return ref;
  });
}

var unlockProducer = function() {}

Handler.prototype.produce = function(data, opts, override) {
  var self = this;
  opts = opts || {};
  debugx.enabled && debugx('produce() an object to rabbitmq');
  return lockProducer.call(self, override).then(function(ref) {
    var sendTo = function() {
      self.config.sendable = ref.channel.publish(ref.exchangeName, ref.routingKey, new Buffer(stringifyData(data)), opts);
    }
    if (self.config.sendable !== false) {
      sendTo();
      debugx.enabled && debugx('Producer channel is writable, msg has been sent');
      return self.config.sendable;
    } else {
      debugx.enabled && debugx('Producer channel is overflowed, waiting');
      return new Promise(function(resolved, rejected) {
        ref.channel.once('drain', function() {
          sendTo();
          debugx.enabled && debugx('Producer channel is drained, flushed');
          resolved(self.config.sendable);
        });
      });
    }
  }).then(function(result) {
    unlockProducer.call(self);
    return result;
  });
}

Handler.prototype.enqueue = Handler.prototype.produce;
Handler.prototype.publish = Handler.prototype.produce;

var getRiverbedState = function() {
  this.riverbedState = this.riverbedState || {};
  if (this.config.exchangeMutex) {
    this.riverbedState.mutex = this.riverbedState.mutex || locks.createMutex();
  }
  return (this.riverbedState);
}

var lockRiverbed = function(override) {
  var self = this;
  var producerState = getProducerState.call(self);
  var riverbedState = getRiverbedState.call(self);
  return retrieveExchange.call({ config: self.config, store: riverbedState }, override).then(function(ref) {
    producerState.fence && producerState.fence.set(false);
    if (riverbedState.mutex) {
      return new Promise(function(resolved, rejected) {
        riverbedState.mutex.lock(function() {
          resolved(ref);
        });
      });
    }
    return ref;
  });
}

var unlockRiverbed = function() {
  var self = this;
  var producerState = getProducerState.call(self);
  var riverbedState = getRiverbedState.call(self);
  producerState.fence && producerState.fence.set(true);
  riverbedState.mutex && riverbedState.mutex.unlock();
}

Handler.prototype.exhaust = function(stream, opts, override) {
  var self = this;
  self.riverbedState = self.riverbedState || {};
  opts = opts || {};
  debugx.enabled && debugx('exhaust() data from a readable stream');
  if (!(stream instanceof Readable)) return Promise.reject({
    message: '[source] is not a readable stream'
  });
  return lockRiverbed.call(self, override).then(function(ref) {
    var count = 0;
    var writable = true;
    var sendNext = function(content) {
      var data = stringifyData(content);
      writable = ref.channel.publish(ref.exchangeName, ref.routingKey, new Buffer(data), opts);
      debug2.enabled && debug2('exhaust() - #%s has been sent, next? %s', count++, writable);
    }
    return new Promise(function(resolved, rejected) {
      debug2.enabled && debug2('exhaust() - starting...');
      stream.on('readable', function() {
        if(writable) {
          debug2.enabled && debug2('exhaust() - next #%s', count);
          sendNext(stream.read());
        } else {
          debug2.enabled && debug2('exhaust() - wait #%s', count);
          ref.channel.on('drain', function() {
            ref.channel.removeAllListeners('drain');
            sendNext(stream.read());
          });
        }
      });
      stream.on('end', function() {
        ref.channel.removeAllListeners('drain');
        debug2.enabled && debug2('exhaust() - stream end. Total: %s', count);
        resolved();
      });
      stream.on('error', function(err) {
        ref.channel.removeAllListeners('drain');
        debug2.enabled && debug2('exhaust() - stream error: %s', JSON.stringify(err));
        rejected(err);
      });
    });
  }).then(function(result) {
    unlockRiverbed.call(self);
    return result;
  });
}

Handler.prototype.checkChain = function() {
  var self = this;
  var consumerCfg = self.config.consumer;
  self.consumerState = self.consumerState || {};
  return getChannel.call({ config: consumerCfg, store: self.consumerState }).then(function(ch) {
    return checkQueue.call({ config: consumerCfg }, ch);
  });
}

Handler.prototype.purgeChain = function() {
  var self = this;
  var consumerCfg = self.config.consumer;
  self.consumerState = self.consumerState || {};
  return getChannel.call({ config: consumerCfg, store: self.consumerState }).then(function(ch) {
    return assertQueue.call({ config: consumerCfg }, ch).then(function() {
      return purgeQueue.call({ config: consumerCfg }, ch);
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
  var consumerCfg = self.config.consumer;
  self.consumerState = self.consumerState || {};
  return sendToQueue.call({ name: 'requeue', config: consumerCfg, store: self.consumerState }, data, opts);
}

Handler.prototype.consume = function(callback) {
  var self = this;

  var consumerCfg = self.config.consumer;
  self.consumerState = self.consumerState || {};
  self.consumerState.count = self.consumerState.count || 0;

  debugx.enabled && debugx('consume() an object from rabbitmq');
  return getChannel.call({ config: consumerCfg, store: self.consumerState }).then(function(ch) {

    if (consumerCfg.prefetch && consumerCfg.prefetch >= 0) {
      debugx.enabled && debugx('consume() - set channel prefetch: %s', consumerCfg.prefetch);
      ch.prefetch(consumerCfg.prefetch, true);
    }

    var ok = assertExchange.call({ config: self.config, store: self.consumerState }, ch);

    ok = ok.then(function() {
      return assertQueue.call({ config: consumerCfg }, ch);
    });

    ok = ok.then(function(qok) {
      var ch_bindQueue = Promise.promisify(ch.bindQueue, {context: ch});
      return ch_bindQueue(qok.queue, self.config.exchangeName, self.config.routingKey, {}).then(function() {
        debugx.enabled && debugx('consume() - queue: %s has been bound', qok.queue);
        return qok;
      });
    });

    ok = ok.then(function(qok) {
      debugx.enabled && debugx('consume() - queue info: %s', JSON.stringify(qok));
      if (consumerCfg.maxSubscribers && consumerCfg.maxSubscribers <= qok.consumerCount) {
        var error = {
          consumerCount: qok.consumerCount,
          maxSubscribers: consumerCfg.maxSubscribers,
          message: 'exceeding quota limits of subscribers'
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
            if (consumerCfg.noAck === false) {
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
      }, {noAck: consumerCfg.noAck});
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
          message: 'exceeding quota limits of subscribers'
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

Handler.prototype.refurbishGarbage = function(msg) {

}

Handler.prototype.eliminateGarbage = function() {

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

var stopPublisher = function() {

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
