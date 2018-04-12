'use strict';

var assert = require('assert');
var Devebot = require('devebot');
var Promise = Devebot.require('bluebird');
var lodash = Devebot.require('lodash');
var pinbug = Devebot.require('pinbug');
var debugx = pinbug('devebot:co:rabbitmq:handler');
var debug0 = pinbug('trace:devebot:co:rabbitmq:handler');
var debug2 = pinbug('devebot:co:rabbitmq:handler:extract');
var amqp = require('amqplib/callback_api');
var locks = require('locks');
var util = require('util');
var Readable = require('stream').Readable;
var Writable = require('stream').Writable;
var zapper = require('./zapper');

var Handler = function(params) {
  debugx.enabled && debugx(' + constructor begin ...');

  var self = this;
  var LX = this.logger || zapper.getLogger();
  var LT = this.tracer || zapper.getTracer();

  var config = {};
  config.uri = params.uri || params.host;
  config.exchangeName = params.exchangeName || params.exchange;
  config.exchangeType = params.exchangeType || 'fanout';
  if (zapper.isNumber(params.exchangeQuota)) {
    config.exchangeQuota = params.exchangeQuota;
  }
  if (zapper.isBoolean(params.exchangeMutex)) {
    config.exchangeMutex = params.exchangeMutex;
  }
  config.durable = zapper.isBoolean(params.durable) ? params.durable : true;
  config.autoDelete = zapper.isBoolean(params.autoDelete) ? params.autoDelete : false;
  if (zapper.isString(params.alternateExchange)) {
    config.alternateExchange = params.alternateExchange;
  }
  config.routingKey = zapper.isString(params.routingKey) ? params.routingKey : '';

  config.consumer = { uri: config.uri };
  if (lodash.isObject(params.consumer)) {
    lodash.merge(config.consumer, params.consumer);
  }

  if (!zapper.isString(config.consumer.queueName)) {
    config.consumer.queueName = params.queueName || params.queue || '';
  }
  if (!zapper.isBoolean(config.consumer.durable)) {
    config.consumer.durable = zapper.isBoolean(params.durable) ? params.durable : true;
  }
  if (!zapper.isBoolean(config.consumer.exclusive)) {
    config.consumer.exclusive = zapper.isBoolean(params.exclusive) ? params.exclusive : false;
  }
  if (!zapper.isBoolean(config.consumer.noAck)) {
    config.consumer.noAck = zapper.isBoolean(params.noAck) ? params.noAck : false;
  }
  if (!zapper.isNumber(config.consumer.prefetch)) {
    config.consumer.prefetch = zapper.isNumber(params.prefetch) ? params.prefetch : undefined;
  }
  if (!zapper.isBoolean(config.consumer.connectIsCached)) {
    config.consumer.connectIsCached = zapper.isBoolean(params.connectIsCached) ? params.connectIsCached : true;
  }
  if (!zapper.isBoolean(config.consumer.channelIsCached)) {
    config.consumer.channelIsCached = zapper.isBoolean(params.channelIsCached) ? params.channelIsCached : true;
  }
  if (!zapper.isNumber(config.consumer.maxSubscribers)) {
    config.consumer.maxSubscribers = params.maxSubscribers;
  }

  if (lodash.isObject(params.recycler)) {
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

let getTicket = function(kwargs) {
  let {store} = kwargs;
  let ticket;
  if (store.guard) {
    ticket = new Promise(function(onResolved, onRejected) {
      store.guard.lock(function() {
        onResolved();
      });
    });
    ticket.finally(function() {
      store.guard.unlock();
    });
  } else {
    ticket = Promise.resolve();
  }
  return ticket;
}

let getConnection = function(kwargs) {
  let {store, config} = kwargs;

  store.connectionCount = store.connectionCount || 0;
  debugx.enabled && debugx('getConnection() - connection amount: %s', store.connectionCount);

  if (config.connectIsCached !== false && store.connection) {
    debugx.enabled && debugx('getConnection() - connection has been available');
    return Promise.resolve(store.connection);
  } else {
    debugx.enabled && debugx('getConnection() - make a new connection');
    let amqp_connect = Promise.promisify(amqp.connect, {context: amqp});
    return amqp_connect(config.uri, {}).then(function(conn) {
      store.connectionCount += 1;
      conn.on('close', function() {
        debugx.enabled && debugx('getConnection() - connection is closed');
        store.connection = undefined;
        store.connectionCount--;
      });
      conn.on('error', function(err) {
        debugx.enabled && debugx('getConnection() - connection has error');
        store.connection = undefined;
      });
      debugx.enabled && debugx('getConnection() - connection is created successfully');
      return (store.connection = conn);
    }).catch(function(err) {
      return Promise.resolve(kwargs).delay(1100).then(getConnection);
    });
  }
}

let getChannel = function(kwargs) {
  let {store, config} = kwargs;
  return getTicket(kwargs).then(function() {
    if (config.channelIsCached !== false && store.channel) {
      debugx.enabled && debugx('getChannel() - channel has been available');
      return Promise.resolve(store.channel);
    } else {
      debugx.enabled && debugx('getChannel() - make a new channel');
      return getConnection(kwargs).then(function(conn) {
        debugx.enabled && debugx('getChannel() - connection has already');
        var createChannel = Promise.promisify(conn.createConfirmChannel, {context: conn});
        return createChannel().then(function(ch) {
          ch.on('close', function() {
            debugx.enabled && debugx('getChannel() - channel is closed');
            store.channel = undefined;
          });
          ch.on('error', function(err) {
            debugx.enabled && debugx('getChannel() - channel has error');
            store.channel = undefined;
          });
          debugx.enabled && debugx('getChannel() - channel is created successfully');
          return (store.channel = ch);
        }).catch(function(err) {
          return Promise.resolve(kwargs).delay(1700).then(getChannel);
        });
      });
    }
  });
}

var assertExchange = function(ch) {
  var self = this;
  if (!self.config.exchangeName) return Promise.resolve();
  if (self.store.exchangeAsserted) return Promise.resolve(self.store.exchangeAsserted);
  var ch_assertExchange = Promise.promisify(ch.assertExchange, {context: ch});
  return ch_assertExchange(self.config.exchangeName, self.config.exchangeType, {
    durable: self.config.durable,
    autoDelete: self.config.autoDelete
  }).then(function(eok) {
    self.store.exchangeAsserted = eok;
    return eok;
  });
}

var retrieveExchange = function(override) {
  var self = this;
  override = override || {};
  return getChannel(self).then(function(ch) {
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
    return new Promise(function(onResolved, onRejected) {
      ch.sendToQueue(self.config.queueName, zapper.bufferify(data), opts, function(err, ok) {
        if (err) {
          onRejected(err);
        } else {
          ok = ok || {};
          ok.drained = true;
          onResolved(ok);
        }
      });
    });
  }
  var sendToDone = function(sok) {
    if (self.config.sendable !== false) {
      debugx.enabled && debugx('%s() channel is writable, msg has been sent', self.name);
    } else {
      debugx.enabled && debugx('%s() channel is drained, flushed', self.name);
    }
    return (self.config.sendable = sok.drained);
  };
  debugx.enabled && debugx('%s() an object to rabbitmq queue', self.name);
  return getChannel({ config: self.config, store: self.store }).then(function(ch) {
    return assertQueue.call({ config: self.config }, ch).then(function() {
      if (self.config.sendable !== false) {
        debugx.enabled && debugx('%s() channel is writable, send now', self.name);
        return sendTo(ch).then(sendToDone);
      } else {
        debugx.enabled && debugx('%s() channel is overflowed, waiting', self.name);
        return new Promise(function(onResolved, onRejected) {
          ch.once('drain', function() {
            sendTo(ch).then(sendToDone).then(onResolved).catch(onRejected);
          });
        });
      }
      return self.config.sendable;
    });
  });
}

Handler.prototype.ready = Handler.prototype.prepare = function() {
  var self = this;
  return getChannel({ config: self.config, store: getProducerState.call(self) }).then(function(ch) {
    return assertExchange.call({ config: self.config, store: getProducerState.call(self) }, ch);
  });
}

var getProducerState = function() {
  this.producerState = this.producerState || {};
  if (!this.producerState.guard) {
    this.producerState.guard = this.producerState.guard || locks.createMutex();
  }
  if (this.config.exchangeMutex && !this.producerState.fence) {
    this.producerState.fence = this.producerState.fence || locks.createCondVariable(true);
  }
  if (this.config.exchangeQuota && !this.producerState.quota) {
    this.producerState.quota = this.producerState.quota || locks.createSemaphore(this.config.exchangeQuota);
  }
  return (this.producerState);
}

var lockProducer = function() {
  var self = this;
  var producerState = getProducerState.call(self);
  var ticket = Promise.resolve(producerState);
  if (producerState.fence) {
    ticket = ticket.then(function(producerState) {
      return new Promise(function(onResolved, onRejected) {
        producerState.fence.wait(
          function conditionTest(value) {
            return value === true;
          },
          function whenConditionMet() {
            onResolved(producerState);
          }
        );
      });
    });
  }
  if (producerState.quota) {
    ticket = ticket.then(function(producerState) {
      return new Promise(function(onResolved, onRejected) {
        producerState.quota.wait(
          function whenResourceAvailable() {
            onResolved(producerState);
          }
        );
      });
    });
  }
  return ticket;
}

var unlockProducer = function() {
  var self = this;
  var producerState = getProducerState.call(self);
  if (producerState.quota) {
    producerState.quota.signal();
  }
}

Handler.prototype.produce = function(data, opts, override) {
  var self = this;
  opts = opts || {};
  debugx.enabled && debugx('produce() an object to rabbitmq');
  return lockProducer.call(self).then(function(producerState) {
    return retrieveExchange.call({ config: self.config, store: producerState }, override).then(function(ref) {
      var sendTo = function() {
        return getChannel({ config: self.config, store: producerState }).then(function(channel) {
          var connection = producerState.connection;
          return new Promise(function(onResolved, onRejected) {
            var unexpectedClosing = function() {
              onRejected({ msg: 'Timeout exception' });
            }
            connection.on('error', unexpectedClosing);
            try {
              channel.publish(ref.exchangeName, ref.routingKey, zapper.bufferify(data), opts, function(err, ok) {
                connection.removeListener('close', unexpectedClosing);
                if (err) {
                  onRejected(err);
                  debugx.enabled && debugx('sendTo() failed: %s', JSON.stringify(err));
                } else {
                  ok = ok || {};
                  ok.drained = true;
                  onResolved(ok);
                  debugx.enabled && debugx('sendTo() is ok');
                }
              });
            } catch(exception) {
              connection.removeListener('close', unexpectedClosing);
              debugx.enabled && debugx('sendTo() throw exception: %s', JSON.stringify(exception));
              onRejected(exception);
            }
          });
        }).catch(function(err) {
          debugx.enabled && debugx('produce() - sendTo() failed, recall ...');
          producerState.connection = null;
          producerState.channel = null;
          return sendTo();
        });
      }
      var sendToDone = function(sok) {
        if (self.config.sendable !== false) {
          debugx.enabled && debugx('Producer channel is writable, msg has been sent');
        } else {
          debugx.enabled && debugx('Producer channel is drained, flushed');
        }
        return (self.config.sendable = sok.drained);
      };
      var doSend = function() {
        if (self.config.sendable !== false) {
          debugx.enabled && debugx('Producer channel is writable, send now');
          return sendTo().then(sendToDone);
        } else {
          debugx.enabled && debugx('Producer channel is overflowed, waiting');
          return new Promise(function(onResolved, onRejected) {
            ref.channel.once('drain', function() {
              sendTo().then(sendToDone).then(onResolved).catch(onRejected);
            });
          });
        }
      }
      return doSend();
    });
  }).finally(function() {
    unlockProducer.call(self);
  });
}

Handler.prototype.enqueue = Handler.prototype.produce;
Handler.prototype.publish = Handler.prototype.produce;

var getPipelineState = function() {
  this.pipelineState = this.pipelineState || getProducerState.call(this);
  if (this.config.exchangeMutex && !this.pipelineState.mutex) {
    this.pipelineState.mutex = this.pipelineState.mutex || locks.createMutex();
  }
  return (this.pipelineState);
}

var lockPipeline = function(override) {
  var self = this;
  var producerState = getProducerState.call(self);
  var pipelineState = getPipelineState.call(self);
  return retrieveExchange.call({ config: self.config, store: pipelineState }, override).then(function(ref) {
    producerState.fence && producerState.fence.set(false);
    if (pipelineState.mutex) {
      return new Promise(function(resolved, rejected) {
        pipelineState.mutex.lock(function() {
          resolved(ref);
        });
      });
    }
    return ref;
  });
}

var unlockPipeline = function() {
  var self = this;
  var producerState = getProducerState.call(self);
  var pipelineState = getPipelineState.call(self);
  producerState.fence && producerState.fence.set(true);
  pipelineState.mutex && pipelineState.mutex.unlock();
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
  self._ctx.writable = this._ref.channel.publish(this._ref.exchangeName, this._ref.routingKey, zapper.bufferify(obj), this._opts);
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
  return lockPipeline.call(self, override).then(function(ref) {
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
    unlockPipeline.call(self);
    return result;
  }).catch(function(error) {
    debug2.enabled && debug2('exhaust() - error occurred: %s', JSON.stringify(error));
    unlockPipeline.call(self);
    return Promise.reject(error);
  });
}

var getConsumerState = function() {
  return (this.consumerState = this.consumerState || {});
}

Handler.prototype.checkChain = function() {
  var vc = { config: this.config.consumer, store: getConsumerState.call(this) };
  return getChannel(vc).then(function(ch) {
    return checkQueue.call({ config: vc.config }, ch);
  });
}

Handler.prototype.purgeChain = function() {
  var vc = { config: this.config.consumer, store: getConsumerState.call(this) };
  return getChannel(vc).then(function(ch) {
    return assertQueue.call({ config: vc.config }, ch).then(function() {
      return purgeQueue.call({ config: vc.config }, ch);
    });
  });
}

var enqueueChain = function(data, opts) {
  var vc = { name: 'enqueueChain', config: this.config.consumer, store: getConsumerState.call(this) };
  return sendToQueue.call(vc, data, opts);
}

Handler.prototype.consume = function(callback) {
  var self = this;

  assert.ok(zapper.isFunction(callback), 'callback should be a function');

  var vc = { config: self.config.consumer, store: getConsumerState.call(this) };
  vc.store.count = vc.store.count || 0;

  debugx.enabled && debugx('consume() - get an object from Chain');
  return getChannel(vc).then(function(ch) {

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
        callback(msg.content, {
          fields: msg.fields,
          properties: msg.properties
        }, function done(err) {
          if (vc.config.noAck !== true) {
            if (err) {
              var rcfg = self.config.recycler;
              var headers = msg.properties && msg.properties.headers || {};
              headers[rcfg.redeliveredCountName] = (headers[rcfg.redeliveredCountName] || 0) + 1;
              if (headers[rcfg.redeliveredCountName] <= rcfg.redeliveredLimit) {
                debugx.enabled && debugx('consume() - enqueueChain message');
                enqueueChain.call(self, msg.content.toString(), {headers: headers});
              } else {
                if (hasRecycler.call(self)) {
                  debugx.enabled && debugx('consume() - enqueueTrash message');
                  enqueueTrash.call(self, msg.content.toString(), {headers: headers});
                }
              }
              ch.nack(msg, false, false);
            } else {
              ch.ack(msg);
            }
          }
          vc.store.count--;
        });
      }, {noAck: (vc.config.noAck === true)});
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
  return assertRecycler.call(this).then(function(vr) {
    return getChannel(vr).then(function(ch) {
      return checkQueue.call({ config: vr.config }, ch);
    });
  });
}

Handler.prototype.purgeTrash = function() {
  return assertRecycler.call(this).then(function(vr) {
    return getChannel(vr).then(function(ch) {
      return assertQueue.call({ config: vr.config }, ch).then(function() {
        return purgeQueue.call({ config: vr.config }, ch);
      });
    });
  });
}

var enqueueTrash = function(data, opts) {
  return assertRecycler.call(this).then(function(vr) {
    vr.name = 'enqueueTrash';
    return sendToQueue.call(vr, data, opts);
  });
}

Handler.prototype.recycle = function(callback) {
  var self = this;

  assert.ok(zapper.isFunction(callback), 'callback should be a function');

  return assertRecycler.call(self).then(function(vr) {
    vr.store.count = vr.store.count || 0;

    debugx.enabled && debugx('recycle() - get an object from Trash');
    return getChannel(vr).then(function(ch) {

      if (vr.config.prefetch && vr.config.prefetch >= 0) {
        debugx.enabled && debugx('recycle() - set channel prefetch: %s', vr.config.prefetch);
        ch.prefetch(vr.config.prefetch, true);
      }

      var ok = assertQueue.call({ config: vr.config }, ch).then(function(qok) {
        debugx.enabled && debugx('recycle() - queue info: %s', JSON.stringify(qok));
        if (vr.config.maxSubscribers && vr.config.maxSubscribers <= qok.consumerCount) {
          var error = {
            consumerCount: qok.consumerCount,
            maxSubscribers: vr.config.maxSubscribers,
            message: 'exceeding quota limits of subscribers'
          }
          debugx.enabled && debugx('recycle() - Error: %s', JSON.stringify(error));
          return Promise.reject(error);
        }
        return Promise.promisify(ch.consume, {context: ch})(qok.queue, function(msg) {
          vr.store.count++;
          debugx.enabled && debugx('recycle() - received message: %s, fields: %s, amount: %s', 
            msg.content, JSON.stringify(msg.fields), vr.store.count);
          callback(msg.content, {
            fields: msg.fields,
            properties: msg.properties
          }, function done(err) {
            if (vr.config.noAck === false) {
              err ? ch.nack(msg) :ch.ack(msg);
            }
            vr.store.count--;
          });
        }, {noAck: vr.config.noAck});
      });

      return ok.then(function(result) {
        debugx.enabled && debugx('recycle() - consumerTag: %s. CTRL+C to exit', result.consumerTag);
        vr.store.consumerTag = result.consumerTag;
        return result;
      });
    });
  });
}

var examineGarbage = function() {
  var self = this;
  return assertRecycler.call(self).then(function(vr) {
    if (vr.store.garbage) return vr.store.garbage;
    return getChannel(vr).then(function(ch) {
      return assertQueue.call({ config: vr.config }, ch).then(function(qok) {
        return Promise.promisify(ch.get, {context: ch})(qok.queue, {});
      }).then(function(msgOrFalse) {
        debugx.enabled && debugx('examineGarbage() - msg: %s', JSON.stringify(msgOrFalse));
        if (msgOrFalse !== false) vr.store.garbage = msgOrFalse;
        return vr.store.garbage;
      });
    });
  });
}

var garbageAction = {};

var discardGarbage = garbageAction['discard'] = function() {
  var self = this;
  return assertRecycler.call(self).then(function(vr) {
    if (!vr.store.garbage) return false;
    return getChannel(vr).then(function(ch) {
      debugx.enabled && debugx('discardGarbage() - nack()');
      ch.nack(vr.store.garbage, false, false);
      vr.store.garbage = undefined;
      return true;
    });
  });
}

var restoreGarbage = garbageAction['restore'] = function() {
  var self = this;
  return assertRecycler.call(self).then(function(vr) {
    if (!vr.store.garbage) return false;
    return getChannel(vr).then(function(ch) {
      debugx.enabled && debugx('restoreGarbage() - nack()');
      ch.nack(vr.store.garbage);
      vr.store.garbage = undefined;
      return true;
    });
  });
}

var recoverGarbage = garbageAction['recover'] = function() {
  var self = this;
  return assertRecycler.call(self).then(function(vr) {
    if (!vr.store.garbage) return false;
    return getChannel(vr).then(function(ch) {
      var msg = vr.store.garbage;
      return enqueueChain.call(self, msg.content.toString(), msg.properties).then(function(result) {
        debugx.enabled && debugx('recoverGarbage() - ack()');
        ch.ack(vr.store.garbage);
        vr.store.garbage = undefined;
        return true;
      });
    });
  });
}

var requeueGarbage = garbageAction['requeue'] = function() {
  var self = this;
  return assertRecycler.call(self).then(function(vr) {
    if (!vr.store.garbage) return false;
    return getChannel(vr).then(function(ch) {
      var msg = vr.store.garbage;
      return enqueueTrash.call(self, msg.content.toString(), msg.properties).then(function(result) {
        debugx.enabled && debugx('requeueGarbage() - ack()');
        ch.ack(vr.store.garbage);
        vr.store.garbage = undefined;
        return true;
      });
    });
  });
}

Handler.prototype.examine = function(callback) {
  var self = this;
  var result = { obtained: 0 };
  result.callback = zapper.isFunction(callback);
  return examineGarbage.call(self).then(function(msg) {
    if (msg) {
      result.obtained = 1;
      if (!result.callback) return result;
      return new Promise(function(resolved, rejected) {
        var copied = lodash.pick(msg, ['content', 'fields', 'properties']);
        callback(copied, function update(action, data) {
          if (zapper.isObject(data)) {
            msg.content = zapper.bufferify(data.content);
            if (zapper.isObject(data.properties)) {
              lodash.merge(msg.properties, data.properties);
            }
          }
          if (zapper.isFunction(garbageAction[action])) {
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
    return stopPublisher.call({store: getPipelineState.call(self) });
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
      var ch_close = Promise.promisify(self.store.channel.close, {
        context: self.store.channel
      });
      return ch_close().then(function(ok) {
        self.store.channel = undefined;
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
        self.store.connection = undefined;
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
