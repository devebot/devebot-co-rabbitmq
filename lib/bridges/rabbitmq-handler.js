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
var Writable = require('stream').Writable;
var util = require('util');

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
        ch.on('close', function() {
          self.store.channel = null;
        });
        debugx.enabled && debugx('getChannel() - channel is created successfully');
        return (self.store.channel = ch);
      });
    });
  }
}

var stringify = function(data) {
  return (typeof(data) === 'string') ? data : JSON.stringify(data);
}

var bufferify = function(str) {
  return new Buffer(str);
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
    self.config.sendable = ch.sendToQueue(self.config.queueName, bufferify(stringify(data)), opts);
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
  this.producerState = this.producerState || {};
  if (this.config.exchangeMutex && !this.producerState.fence) {
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
      self.config.sendable = ref.channel.publish(ref.exchangeName, ref.routingKey, bufferify(stringify(data)), opts);
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
  this.riverbedState = this.riverbedState || getProducerState.call(this);
  if (this.config.exchangeMutex && !this.riverbedState.mutex) {
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

var SubscriberStream = function(ref, opts) {
  Writable.call(this, {objectMode: true});
  this._ctx = { count: 0, writable: true };
  this._ref = ref || {};
  this._opts = opts || {};
}

util.inherits(SubscriberStream, Writable);

SubscriberStream.prototype._write = function(chunk, encoding, callback) {
  var self = this;
  if (self._ctx.writable) {
    self._next(chunk, callback);
  } else {
    self._ref.channel.once('drain', function() {
      self._next(chunk, callback);
    });
  }
}

SubscriberStream.prototype._next = function(obj, callback) {
  var self = this;
  self._ctx.writable = this._ref.channel.publish(this._ref.exchangeName, this._ref.routingKey, bufferify(stringify(obj)), this._opts);
  debug2.enabled && debug2('exhaust() - #%s has been sent, next? %s', self._ctx.count++, self._ctx.writable);
  callback();
}

SubscriberStream.prototype.count = function() {
  return this._ctx.count;
}

Handler.prototype.exhaust = function(stream, opts, override) {
  var self = this;
  opts = opts || {};
  debugx.enabled && debugx('exhaust() data from a readable stream');
  if (!(stream instanceof Readable)) return Promise.reject({
    message: '[source] is not a readable stream'
  });
  return lockRiverbed.call(self, override).then(function(ref) {
    var subscriberStream = new SubscriberStream(ref, opts);
    return new Promise(function(resolved, rejected) {
      debug2.enabled && debug2('exhaust() - starting...');
      subscriberStream.on('finish', function() {
        debug2.enabled && debug2('exhaust() - stream end. Total: %s', subscriberStream.count());
        resolved();
      });
      subscriberStream.on('error', function(err) {
        debug2.enabled && debug2('exhaust() - stream error: %s', JSON.stringify(err));
        rejected(err);
      });
      stream.pipe(subscriberStream, { end: true });
    });
  }).then(function(result) {
    debug2.enabled && debug2('exhaust() - finish successfully');
    unlockRiverbed.call(self);
    return result;
  }).catch(function(error) {
    debug2.enabled && debug2('exhaust() - error occurred: %s', JSON.stringify(error));
    unlockRiverbed.call(self);
    return Promise.reject(error);
  });
}

var getConsumerState = function() {
  return (this.consumerState = this.consumerState || {});
}

Handler.prototype.checkChain = function() {
  var v = { config: this.config.consumer, store: getConsumerState.call(this) };
  return getChannel.call(v).then(function(ch) {
    return checkQueue.call({ config: v.config }, ch);
  });
}

Handler.prototype.purgeChain = function() {
  var v = { config: this.config.consumer, store: getConsumerState.call(this) };
  return getChannel.call(v).then(function(ch) {
    return assertQueue.call({ config: v.config }, ch).then(function() {
      return purgeQueue.call({ config: v.config }, ch);
    });
  });
}

var enqueueChain = function(data, opts) {
  var v = { name: 'enqueueChain', config: this.config.consumer, store: getConsumerState.call(this) };
  return sendToQueue.call(v, data, opts);
}

Handler.prototype.consume = function(callback) {
  var self = this;

  var vc = { config: self.config.consumer, store: getConsumerState.call(this) };
  vc.store.count = vc.store.count || 0;

  debugx.enabled && debugx('consume() - get an object from Chain');
  return getChannel.call(vc).then(function(ch) {

    if (vc.config.prefetch && vc.config.prefetch >= 0) {
      debugx.enabled && debugx('consume() - set channel prefetch: %s', vc.config.prefetch);
      ch.prefetch(vc.config.prefetch, true);
    }

    var ok = assertExchange.call({ config: self.config, store: vc.store }, ch);

    ok = ok.then(function() {
      return assertQueue.call({ config: vc.config }, ch);
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
      if (vc.config.maxSubscribers && vc.config.maxSubscribers <= qok.consumerCount) {
        var error = {
          consumerCount: qok.consumerCount,
          maxSubscribers: vc.config.maxSubscribers,
          message: 'exceeding quota limits of subscribers'
        }
        debugx.enabled && debugx('consume() - Error: %s', JSON.stringify(error));
        return Promise.reject(error);
      }
      var ch_consume = Promise.promisify(ch.consume, {context: ch});
      return ch_consume(qok.queue, function(msg) {
        vc.store.count++;
        debug0.enabled && debug0('consume() - received message: %s, fields: %s, amount: %s', 
          msg.content, JSON.stringify(msg.fields), vc.store.count);
        if (callback && typeof(callback) === 'function') {
          callback(msg.content, {
            fields: msg.fields,
            properties: msg.properties
          }, function done(err) {
            if (vc.config.noAck === false) {
              if (hasRecycler.call(self)) {
                if (err) {
                  var recyclerCfg = self.config.recycler;
                  var headers = msg.properties && msg.properties.headers || {};
                  headers[recyclerCfg.redeliveredCountName] = (headers[recyclerCfg.redeliveredCountName] || 0) + 1;
                  if (headers[recyclerCfg.redeliveredCountName] <= recyclerCfg.redeliveredLimit) {
                    debugx.enabled && debugx('consume() - enqueueChain message');
                    enqueueChain.call(self, msg.content.toString(), {headers: headers});
                  } else {
                    debugx.enabled && debugx('consume() - enqueueTrash message');
                    enqueueTrash.call(self, msg.content.toString(), {headers: headers});
                  }
                  ch.ack(msg);
                } else {
                  ch.ack(msg);
                }
              } else {
                err ? ch.nack(msg) :ch.ack(msg);
              }
            }
            vc.store.count--;
          });
        } else {
          debugx.enabled && debugx('consume() - callback (observer) is not available');
        }
      }, {noAck: vc.config.noAck});
    });

    return ok.then(function(result) {
      debugx.enabled && debugx('consume() - consumerTag: %s. CTRL+C to exit', result.consumerTag);
      vc.store.consumerTag = result.consumerTag;
      return result;
    });
  });
}

Handler.prototype.process = Handler.prototype.consume;

var getRecyclerState = function() {
  return (this.recyclerState = this.recyclerState || {});
}

var hasRecycler = function() {
  var recyclerCfg = this.config.recycler;
  return recyclerCfg && (recyclerCfg.enabled !== false);
}

var assertRecycler = function() {
  var recyclerCfg = this.config.recycler;
  if (!recyclerCfg || (recyclerCfg.enabled === false)) return Promise.reject({
    message: 'The recycler is unavailable'
  });
  return Promise.resolve({ config: recyclerCfg, store: getRecyclerState.call(this) });
}

Handler.prototype.checkTrash = function() {
  return assertRecycler.call(this).then(function(v) {
    return getChannel.call(v).then(function(ch) {
      return checkQueue.call({ config: v.config }, ch);
    });
  });
}

Handler.prototype.purgeTrash = function() {
  return assertRecycler.call(this).then(function(v) {
    return getChannel.call(v).then(function(ch) {
      return assertQueue.call({ config: v.config }, ch).then(function() {
        return purgeQueue.call({ config: v.config }, ch);
      });
    });
  });
}

var enqueueTrash = function(data, opts) {
  return assertRecycler.call(this).then(function(v) {
    v.name = 'enqueueTrash';
    return sendToQueue.call(v, data, opts);
  });
}

Handler.prototype.recycle = function(callback) {
  var self = this;
  return assertRecycler.call(self).then(function(v) {
    v.store.count = v.store.count || 0;

    debugx.enabled && debugx('recycle() - get an object from Trash');
    return getChannel.call(v).then(function(ch) {

      if (v.config.prefetch && v.config.prefetch >= 0) {
        debugx.enabled && debugx('recycle() - set channel prefetch: %s', v.config.prefetch);
        ch.prefetch(v.config.prefetch, true);
      }

      var ok = assertQueue.call({ config: v.config }, ch).then(function(qok) {
        debugx.enabled && debugx('recycle() - queue info: %s', JSON.stringify(qok));
        if (v.config.maxSubscribers && v.config.maxSubscribers <= qok.consumerCount) {
          var error = {
            consumerCount: qok.consumerCount,
            maxSubscribers: v.config.maxSubscribers,
            message: 'exceeding quota limits of subscribers'
          }
          debugx.enabled && debugx('recycle() - Error: %s', JSON.stringify(error));
          return Promise.reject(error);
        }
        return Promise.promisify(ch.consume, {context: ch})(qok.queue, function(msg) {
          v.store.count++;
          debugx.enabled && debugx('recycle() - received message: %s, fields: %s, amount: %s', 
            msg.content, JSON.stringify(msg.fields), v.store.count);
          if (callback && typeof(callback) === 'function') {
            callback(msg.content, {
              fields: msg.fields,
              properties: msg.properties
            }, function done(err) {
              if (v.config.noAck === false) {
                err ? ch.nack(msg) :ch.ack(msg);
              }
              v.store.count--;
            });
          } else {
            debugx.enabled && debugx('recycle() - callback (observer) is not available');
          }
        }, {noAck: v.config.noAck});
      });

      return ok.then(function(result) {
        debugx.enabled && debugx('recycle() - consumerTag: %s. CTRL+C to exit', result.consumerTag);
        v.store.consumerTag = result.consumerTag;
        return result;
      });
    });
  });
}

var examineGarbage = function() {
  var self = this;
  return assertRecycler.call(self).then(function(v) {
    if (v.store.garbage) return v.store.garbage;
    return getChannel.call(v).then(function(ch) {
      return assertQueue.call({ config: v.config }, ch).then(function(qok) {
        return Promise.promisify(ch.get, {context: ch})(qok.queue, {});
      }).then(function(msgOrFalse) {
        debugx.enabled && debugx('examineGarbage() - msg: %s', JSON.stringify(msgOrFalse));
        if (msgOrFalse !== false) v.store.garbage = msgOrFalse;
        return v.store.garbage;
      });
    });
  });
}

var garbageAction = {};

var discardGarbage = garbageAction['discard'] = function() {
  var self = this;
  return assertRecycler.call(self).then(function(v) {
    if (!v.store.garbage) return false;
    return getChannel.call(v).then(function(ch) {
      debugx.enabled && debugx('discardGarbage() - nack()');
      ch.nack(v.store.garbage, false, false);
      v.store.garbage = undefined;
      return true;
    });
  });
}

var restoreGarbage = garbageAction['restore'] = function() {
  var self = this;
  return assertRecycler.call(self).then(function(v) {
    if (!v.store.garbage) return false;
    return getChannel.call(v).then(function(ch) {
      debugx.enabled && debugx('restoreGarbage() - nack()');
      ch.nack(v.store.garbage);
      v.store.garbage = undefined;
      return true;
    });
  });
}

var recoverGarbage = garbageAction['recover'] = function() {
  var self = this;
  return assertRecycler.call(self).then(function(v) {
    if (!v.store.garbage) return false;
    return getChannel.call(v).then(function(ch) {
      var msg = v.store.garbage;
      return enqueueChain.call(self, msg.content.toString(), msg.properties).then(function(result) {
        debugx.enabled && debugx('recoverGarbage() - ack()');
        ch.ack(v.store.garbage);
        v.store.garbage = undefined;
        return true;
      });
    });
  });
}

var requeueGarbage = garbageAction['requeue'] = function() {
  var self = this;
  return assertRecycler.call(self).then(function(v) {
    if (!v.store.garbage) return false;
    return getChannel.call(v).then(function(ch) {
      var msg = v.store.garbage;
      return enqueueTrash.call(self, msg.content.toString(), msg.properties).then(function(result) {
        debugx.enabled && debugx('requeueGarbage() - ack()');
        ch.ack(v.store.garbage);
        v.store.garbage = undefined;
        return true;
      });
    });
  });
}

Handler.prototype.examine = function(callback) {
  var self = this;
  var result = { obtained: 0 };
  result.callback = lodash.isFunction(callback);
  return examineGarbage.call(self).then(function(msg) {
    if (msg) {
      result.obtained = 1;
      if (!result.callback) return result;
      return new Promise(function(resolved, rejected) {
        var copied = lodash.pick(msg, ['content', 'fields', 'properties']);
        callback(copied, function update(action, data) {
          if (data && lodash.isObject(data)) {
            if(data.content instanceof Buffer) {
              msg.content = data.content;
            } else if (typeof(data.content) === 'string') {
              msg.content = bufferify(data.content);
            } else if (typeof(data.content) === 'object') {
              msg.content = bufferify(stringify(data.content));
            }
            if (data.properties && lodash.isObject(data.properties)) {
              lodash.merge(msg.properties, data.properties);
            }
          }
          if (lodash.isFunction(garbageAction[action])) {
            result.nextAction = true;
            garbageAction[action].call(self).then(function() {
              resolved(result);
            }).catch(function(err) {
              result.actionError = err;
              rejected(result);
            });
          } else {
            result.nextAction = false;
            resolved(result);
          }
        });
      });
    } else {
      return result;
    }
  });
}

Handler.prototype.destroy = function() {
  var self = this;
  return Promise.resolve().then(function() {
    return stopPublisher.call({store: getProducerState.call(self) });
  }).then(function() {
    return stopPublisher.call({store: getRiverbedState.call(self) });
  }).then(function() {
    return stopSubscriber.call({store: getConsumerState.call(self) });
  }).then(function() {
    return stopSubscriber.call({store: getRecyclerState.call(self) });
  });
}

var stopPublisher = function() {
  var self = this;
  return Promise.resolve().then(function() {
    if (self.store.channel) {
      self.store.channel.removeAllListeners('drain');
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

var stopSubscriber = function() {
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
