'use strict';

const assert = require('assert');
const Devebot = require('devebot');
const Promise = Devebot.require('bluebird');
const lodash = Devebot.require('lodash');
const pinbug = Devebot.require('pinbug');
const debugx = pinbug('devebot:co:rabbitmq:handler');
const debug0 = pinbug('trace:devebot:co:rabbitmq:handler');
const debug2 = pinbug('devebot:co:rabbitmq:handler:extract');
const amqp = require('amqplib/callback_api');
const locks = require('locks');
const util = require('util');
const Readable = require('stream').Readable;
const Writable = require('stream').Writable;
const zapper = require('./zapper');

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
  if (!zapper.isBoolean(config.consumer.autoBinding)) {
    config.consumer.autoBinding = true;
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
        delete store.connection;
        store.connectionCount--;
      });
      conn.on('error', function(err) {
        debugx.enabled && debugx('getConnection() - connection has error');
        delete store.connection;
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
  if (config.channelIsCached !== false && store.channel) {
    debugx.enabled && debugx('getChannel() - channel has been available');
    return Promise.resolve(store.channel);
  }
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
            delete store.channel;
          });
          ch.on('error', function(err) {
            debugx.enabled && debugx('getChannel() - channel has error');
            delete store.channel;
          });
          ch.on('drain', function() {
            debugx.enabled && debugx('getChannel() - channel has drained, open the valve');
            store.valve && store.valve.set(true);
          })
          debugx.enabled && debugx('getChannel() - channel is created successfully');
          return (store.channel = ch);
        }).catch(function(err) {
          return Promise.resolve(kwargs).delay(1700).then(getChannel);
        });
      });
    }
  });
}

let retrieveExchange = function(kwargs, override) {
  let {config} = kwargs;
  override = override || {};
  return getChannel(kwargs).then(function(ch) {
    let ok = assertExchange(kwargs, ch);
    return ok.then(function(eok) {
      return {
        exchangeName: eok.exchange || config.exchangeName,
        routingKey: override.routingKey || config.routingKey
      };
    });
  });
}

let assertExchange = function(kwargs, ch) {
  let {store, config} = kwargs;
  if (store.exchangeAsserted) return Promise.resolve(store.exchangeAsserted);
  let ch_assertExchange = Promise.promisify(ch.assertExchange, {context: ch});
  return ch_assertExchange(config.exchangeName, config.exchangeType, {
    durable: config.durable,
    autoDelete: config.autoDelete
  }).then(function(eok) {
    return (store.exchangeAsserted = eok);
  });
}

let assertQueue = function(kwargs, ch) {
  let {store, config} = kwargs;
  return Promise.promisify(ch.assertQueue, {context: ch})(config.queueName, {
    durable: config.durable,
    exclusive: config.exclusive
  }).then(function(qok) {
    config.queueName = config.queueName || qok.queue;
    return qok;
  });
}

let checkQueue = function(kwargs, ch) {
  return Promise.promisify(ch.checkQueue, {context: ch})(kwargs.config.queueName);
}

let purgeQueue = function(kwargs, ch) {
  return Promise.promisify(ch.purgeQueue, {context: ch})(kwargs.config.queueName);
}

let sendToQueue = function(kwargs, data, opts) {
  let {store, config} = kwargs;
  opts = opts || {};
  let sendTo = function() {
    return getChannel(kwargs).then(function(ch) {
      let connection = store.connection;
      return new Promise(function(onResolved, onRejected) {
        let unexpectedClosing = function() {
          onRejected({ msg: 'Timeout exception' });
        }
        connection.on('error', unexpectedClosing);
        try {
          ch.sendToQueue(config.queueName, zapper.bufferify(data), opts, function(err, ok) {
            connection.removeListener('close', unexpectedClosing);
            if (err) {
              onRejected(err);
            } else {
              ok = ok || {};
              ok.drained = true;
              onResolved(ok);
            }
          });
        } catch(exception) {
          connection.removeListener('close', unexpectedClosing);
          onRejected(exception);
        }
      });
    }).catch(function(err) {
      debugx.enabled && debugx('sendToQueue() - sendTo() failed, recall ...');
      store.connection = null;
      store.channel = null;
      return sendTo();
    });
  }
  debugx.enabled && debugx('%s() an object to rabbitmq queue', kwargs.name);
  return getChannel(kwargs).then(function(ch) {
    return assertQueue(kwargs, ch).then(function() {
      return sendTo();
    });
  });
}

let checkValve = function(ctx, whenConditionMet) {
  let {store} = ctx;
  if (store.valve) {
    store.valve.wait(
      function conditionTest(value) {
        return value === true;
      },
      whenConditionMet
    );
  } else {
    whenConditionMet();
  }
}

let sendToExchange = function(ctx, data, opts, target) {
  return getChannel(ctx).then(function(channel) {
    return new Promise(function(onResolved, onRejected) {
      let unexpectedClosing = function() {
        onRejected({ msg: 'Timeout exception' });
      }
      let connection = ctx.store.connection;
      connection.on('error', unexpectedClosing);
      try {
        checkValve(ctx, function() {
          let drained = channel.publish(target.exchangeName, target.routingKey, zapper.bufferify(data), opts, function(err, ok) {
            connection.removeListener('close', unexpectedClosing);
            if (err) {
              onRejected(err);
              debugx.enabled && debugx('sendToExchange() failed: %s', JSON.stringify(err));
            } else {
              ok = ok || {};
              ok.drained = true;
              onResolved(ok);
              debugx.enabled && debugx('sendToExchange() is ok');
            }
          });
          if (!drained) {
            debugx.enabled && debugx('channel is overflowed, close the valve');
            ctx.store.valve && ctx.store.valve.set(false);
          }
        });
      } catch(exception) {
        connection.removeListener('close', unexpectedClosing);
        debugx.enabled && debugx('sendToExchange() throw exception: %s', JSON.stringify(exception));
        onRejected(exception);
      }
    });
  }).catch(function(err) {
    debugx.enabled && debugx('produce() - sendToExchange() failed, recall ...');
    ctx.store.connection = null;
    ctx.store.channel = null;
    return sendToExchange(ctx, data, opts, target);
  });
}

let getProducerState = function(self) {
  self = self || this;
  self.producerState = self.producerState || {};
  self.producerState.guard = self.producerState.guard || locks.createMutex();
  self.producerState.valve = self.producerState.valve || locks.createCondVariable(true);
  if (self.config.exchangeMutex) {
    self.producerState.fence = self.producerState.fence || locks.createCondVariable(true);
  }
  if (self.config.exchangeQuota) {
    self.producerState.quota = self.producerState.quota || locks.createSemaphore(self.config.exchangeQuota);
  }
  return (self.producerState);
}

let lockProducer = function(self) {
  self = self || this;
  let producerState = getProducerState(self);
  let ticket = Promise.resolve(producerState);
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

let unlockProducer = function(self) {
  self = self || this;
  let producerState = getProducerState(self);
  if (producerState.quota) {
    producerState.quota.signal();
  }
}

Handler.prototype.ready = Handler.prototype.prepare = function() {
  let sandbox = { config: this.config, store: getProducerState(this) };
  return getChannel(sandbox).then(function(ch) {
    return assertExchange(sandbox, ch);
  });
}

Handler.prototype.produce = function(data, opts, override) {
  let self = this;
  opts = opts || {};
  debugx.enabled && debugx('produce() an object to rabbitmq');
  return lockProducer(self).then(function(producerState) {
    let ctx = { config: self.config, store: producerState };
    return retrieveExchange(ctx, override).then(function(ref) {
      return sendToExchange(ctx, data, opts, ref);
    });
  }).finally(function() {
    unlockProducer(self);
  });
}

Handler.prototype.enqueue = Handler.prototype.produce;
Handler.prototype.publish = Handler.prototype.produce;

let getPipelineState = function(self) {
  self = self || this;
  self.pipelineState = self.pipelineState || {};
  self.pipelineState.guard = self.pipelineState.guard || locks.createMutex();
  self.pipelineState.valve = self.pipelineState.valve || locks.createCondVariable(true);
  if (self.config.exchangeMutex) {
    self.pipelineState.mutex = self.pipelineState.mutex || locks.createMutex();
  }
  return (self.pipelineState);
}

let lockPipeline = function(self) {
  self = self || this;
  let producerState = getProducerState(self);
  if (producerState.fence) {
    producerState.fence.set(false);
  }
  let pipelineState = getPipelineState(self);
  return new Promise(function(onResolved, onRejected) {
    if (pipelineState.mutex) {
      pipelineState.mutex.lock(function() {
        onResolved(pipelineState);
      });
    } else {
      onResolved(pipelineState);
    }
  });
}

let unlockPipeline = function(self) {
  self = self || this;
  let producerState = getProducerState(self);
  let pipelineState = getPipelineState(self);
  producerState.fence && producerState.fence.set(true);
  pipelineState.mutex && pipelineState.mutex.unlock();
}

let ProducerStream = function(context, options, target) {
  options = options || {};
  Writable.call(this, {objectMode: (options.objectMode !== false)});

  let count = 0;
  this.count = function() {
    return count;
  }

  this._write = function(chunk, encoding, callback) {
    debug2.enabled && debug2('ProducerStream._write() - sending #%s', count + 1);
    sendToExchange(context, chunk, options, target).then(function() {
      count++;
      debug2.enabled && debug2('ProducerStream._write() - #%s has been sent', count);
      callback();
    });
  }
}

util.inherits(ProducerStream, Writable);

Handler.prototype.exhaust = function(stream, opts, override) {
  var self = this;
  opts = opts || {};
  debugx.enabled && debugx('exhaust() data from a readable stream');
  if (!(stream instanceof Readable)) return Promise.reject({
    message: '[source] is not a readable stream'
  });
  return lockPipeline(self).then(function(pipelineState) {
    var ctx = { config: self.config, store: pipelineState };
    return retrieveExchange(ctx, override).then(function(ref) {
      var producerStream = new ProducerStream(ctx, opts, ref);
      return new Promise(function(resolved, rejected) {
        debug2.enabled && debug2('exhaust() - starting...');
        producerStream.on('error', function(err) {
          debug2.enabled && debug2('exhaust() - stream error: %s', JSON.stringify(err));
          rejected(err);
        });
        producerStream.on('finish', function() {
          debug2.enabled && debug2('exhaust() - stream end. Total: %s', producerStream.count());
          resolved();
        });
        stream.pipe(producerStream, { end: true });
      });
    });
  }).then(function(result) {
    debug2.enabled && debug2('exhaust() - finish successfully');
    return result;
  }).catch(function(error) {
    debug2.enabled && debug2('exhaust() - error occurred: %s', JSON.stringify(error));
    return Promise.reject(error);
  }).finally(function() {
    unlockPipeline(self);
  });
}

var getConsumerState = function(self) {
  self = self || this;
  return (self.consumerState = self.consumerState || {});
}

var enqueueInbox = function(self, data, opts) {
  var vc = { name: 'enqueueInbox', config: self.config.consumer, store: getConsumerState(self) };
  return sendToQueue(vc, data, opts);
}

Handler.prototype.checkInbox = function() {
  var vc = { config: this.config.consumer, store: getConsumerState(this) };
  return getChannel(vc).then(function(ch) {
    return checkQueue(vc, ch);
  });
}

Handler.prototype.purgeInbox = function() {
  var vc = { config: this.config.consumer, store: getConsumerState(this) };
  return getChannel(vc).then(function(ch) {
    return assertQueue(vc, ch).then(function() {
      return purgeQueue(vc, ch);
    });
  });
}

Handler.prototype.consume = function(callback) {
  var self = this;

  assert.ok(zapper.isFunction(callback), 'callback should be a function');

  var vc = { config: self.config.consumer, store: getConsumerState(this) };
  vc.store.count = vc.store.count || 0;

  debugx.enabled && debugx('consume() - subscribe the Inbox queue');
  return getChannel(vc).then(function(ch) {

    if (vc.config.prefetch && vc.config.prefetch >= 0) {
      debugx.enabled && debugx('consume() - set channel prefetch: %s', vc.config.prefetch);
      ch.prefetch(vc.config.prefetch, true);
    }

    var ok = assertExchange({ config: self.config, store: vc.store }, ch);

    ok = ok.then(function() {
      return assertQueue(vc, ch);
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
      return qok;
    });

    ok = ok.then(function(qok) {
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
                debugx.enabled && debugx('consume() - enqueueInbox message');
                enqueueInbox(self, msg.content.toString(), {headers: headers});
              } else {
                if (hasRecycler(self)) {
                  debugx.enabled && debugx('consume() - enqueueTrash message');
                  enqueueTrash(self, msg.content.toString(), {headers: headers});
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

let getRecyclerState = function(self) {
  self = self || this;
  return (self.recyclerState = self.recyclerState || {});
}

let hasRecycler = function(self) {
  self = self || this;
  let recyclerCfg = self.config.recycler;
  return recyclerCfg && (recyclerCfg.enabled !== false);
}

let assertRecycler = function(self) {
  self = self || this;
  let recyclerCfg = self.config.recycler;
  if (!recyclerCfg || (recyclerCfg.enabled === false)) return Promise.reject({
    message: 'The recycler is unavailable'
  });
  return Promise.resolve({ config: recyclerCfg, store: getRecyclerState(self) });
}

let enqueueTrash = function(self, data, opts) {
  return assertRecycler(self).then(function(vr) {
    vr.name = 'enqueueTrash';
    return sendToQueue(vr, data, opts);
  });
}

Handler.prototype.checkTrash = function() {
  return assertRecycler(this).then(function(vr) {
    return getChannel(vr).then(function(ch) {
      return checkQueue(vr, ch);
    });
  });
}

Handler.prototype.purgeTrash = function() {
  return assertRecycler(this).then(function(vr) {
    return getChannel(vr).then(function(ch) {
      return assertQueue(vr, ch).then(function() {
        return purgeQueue(vr, ch);
      });
    });
  });
}

Handler.prototype.recycle = function(callback) {
  var self = this;

  assert.ok(zapper.isFunction(callback), 'callback should be a function');

  return assertRecycler(self).then(function(vr) {
    vr.store.count = vr.store.count || 0;

    debugx.enabled && debugx('recycle() - get an object from Trash');
    return getChannel(vr).then(function(ch) {

      if (vr.config.prefetch && vr.config.prefetch >= 0) {
        debugx.enabled && debugx('recycle() - set channel prefetch: %s', vr.config.prefetch);
        ch.prefetch(vr.config.prefetch, true);
      }

      var ok = assertQueue(vr, ch).then(function(qok) {
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

var examineGarbage = function(self) {
  self = self || this;
  return assertRecycler(self).then(function(vr) {
    if (vr.store.garbage) return vr.store.garbage;
    return getChannel(vr).then(function(ch) {
      return assertQueue(vr, ch).then(function(qok) {
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

var discardGarbage = garbageAction['discard'] = function(self) {
  self = self || this;
  return assertRecycler(self).then(function(vr) {
    if (!vr.store.garbage) return false;
    return getChannel(vr).then(function(ch) {
      debugx.enabled && debugx('discardGarbage() - nack()');
      ch.nack(vr.store.garbage, false, false);
      vr.store.garbage = undefined;
      return true;
    });
  });
}

var restoreGarbage = garbageAction['restore'] = function(self) {
  self = self || this;
  return assertRecycler(self).then(function(vr) {
    if (!vr.store.garbage) return false;
    return getChannel(vr).then(function(ch) {
      debugx.enabled && debugx('restoreGarbage() - nack()');
      ch.nack(vr.store.garbage);
      vr.store.garbage = undefined;
      return true;
    });
  });
}

var recoverGarbage = garbageAction['recover'] = function(self) {
  self = self || this;
  return assertRecycler(self).then(function(vr) {
    if (!vr.store.garbage) return false;
    return getChannel(vr).then(function(ch) {
      var msg = vr.store.garbage;
      return enqueueInbox(self, msg.content.toString(), msg.properties).then(function(result) {
        debugx.enabled && debugx('recoverGarbage() - ack()');
        ch.ack(vr.store.garbage);
        vr.store.garbage = undefined;
        return true;
      });
    });
  });
}

var requeueGarbage = garbageAction['requeue'] = function(self) {
  self = self || this;
  return assertRecycler(self).then(function(vr) {
    if (!vr.store.garbage) return false;
    return getChannel(vr).then(function(ch) {
      var msg = vr.store.garbage;
      return enqueueTrash(self, msg.content.toString(), msg.properties).then(function(result) {
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
  return examineGarbage(self).then(function(msg) {
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
            garbageAction[action](self).then(function() {
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
    return stopPublisher({store: getProducerState(self) });
  }).then(function() {
    return stopPublisher({store: getPipelineState(self) });
  }).then(function() {
    return stopSubscriber({store: getConsumerState(self) });
  }).then(function() {
    return stopSubscriber({store: getRecyclerState(self) });
  });
}

var stopPublisher = function(self) {
  self = self || this;
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

var stopSubscriber = function(self) {
  self = self || this;
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
