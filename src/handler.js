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

let Handler = function(params) {
  debugx.enabled && debugx(' + constructor begin ...');

  let self = this;
  let LX = this.logger || zapper.getLogger();
  let LT = this.tracer || zapper.getTracer();

  let config = {};
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

  let originalInbox = params.inbox || params.consumer;
  config.inbox = { uri: config.uri };
  if (lodash.isObject(originalInbox)) {
    lodash.merge(config.inbox, originalInbox);
  }

  if (!zapper.isString(config.inbox.queueName)) {
    config.inbox.queueName = params.queueName || params.queue || '';
  }
  if (!zapper.isBoolean(config.inbox.durable)) {
    config.inbox.durable = zapper.isBoolean(params.durable) ? params.durable : true;
  }
  if (!zapper.isBoolean(config.inbox.exclusive)) {
    config.inbox.exclusive = zapper.isBoolean(params.exclusive) ? params.exclusive : false;
  }
  if (!zapper.isBoolean(config.inbox.noAck)) {
    config.inbox.noAck = zapper.isBoolean(params.noAck) ? params.noAck : false;
  }
  if (!zapper.isNumber(config.inbox.prefetch)) {
    config.inbox.prefetch = zapper.isNumber(params.prefetch) ? params.prefetch : undefined;
  }
  if (!zapper.isBoolean(config.inbox.connectIsCached)) {
    config.inbox.connectIsCached = zapper.isBoolean(params.connectIsCached) ? params.connectIsCached : true;
  }
  if (!zapper.isBoolean(config.inbox.channelIsCached)) {
    config.inbox.channelIsCached = zapper.isBoolean(params.channelIsCached) ? params.channelIsCached : true;
  }
  if (!zapper.isBoolean(config.inbox.noBinding)) {
    config.inbox.noBinding = false;
  }
  if (!zapper.isNumber(config.inbox.maxSubscribers)) {
    config.inbox.maxSubscribers = params.maxSubscribers;
  }

  let originalTrash = params.trash || params.recycler;
  if (lodash.isObject(originalTrash)) {
    config.trash = {
      uri: config.uri,
      redeliveredCountName: 'x-redelivered-count',
      redeliveredLimit: 5,
    }
    lodash.merge(config.trash, originalTrash);
    if (lodash.isEmpty(config.trash.queueName)) {
      config.trash.queueName = config.inbox.queueName + '-trashed';
    }
  }

  config.maxListeners = 20;
  if (zapper.isPositiveInteger(params.maxListeners)) {
    config.maxListeners = params.maxListeners;
  }
  if (config.inbox) {
    config.inbox.maxListeners = config.inbox.maxListeners || config.maxListeners;
  }
  if (config.chain) {
    config.chain.maxListeners = config.chain.maxListeners || config.maxListeners;
  }
  if (config.trash) {
    config.trash.maxListeners = config.trash.maxListeners || config.maxListeners;
  }

  debugx.enabled && debugx(' - configuration object: %s', JSON.stringify(config));

  Object.defineProperty(this, 'config', {
    get: function() { return config; },
    set: function(value) {}
  });

  debugx.enabled && debugx(' - constructor end!');
};

let getTicket = function(kwargs) {
  let {state} = kwargs;
  let ticket;
  if (state.guard) {
    ticket = new Promise(function(onResolved, onRejected) {
      state.guard.lock(function() {
        onResolved();
      });
    });
    ticket.finally(function() {
      state.guard.unlock();
    });
  } else {
    ticket = Promise.resolve();
  }
  return ticket;
}

let getConnection = function(kwargs) {
  let {state, config} = kwargs;

  state.connectionCount = state.connectionCount || 0;
  debugx.enabled && debugx('getConnection() - connection amount: %s', state.connectionCount);

  if (config.connectIsCached !== false && state.connection) {
    debugx.enabled && debugx('getConnection() - connection has been available');
    return Promise.resolve(state.connection);
  } else {
    debugx.enabled && debugx('getConnection() - make a new connection');
    let amqp_connect = Promise.promisify(amqp.connect, {context: amqp});
    return amqp_connect(config.uri, {}).then(function(conn) {
      state.connectionCount += 1;
      if (config.maxListeners > 0) {
        conn.setMaxListeners(config.maxListeners);
      }
      conn.on('close', function() {
        debugx.enabled && debugx('getConnection() - connection is closed');
        delete state.connection;
        state.connectionCount--;
      });
      conn.on('error', function(err) {
        debugx.enabled && debugx('getConnection() - connection has error');
        delete state.connection;
      });
      debugx.enabled && debugx('getConnection() - connection is created successfully');
      return (state.connection = conn);
    }).catch(function(err) {
      return Promise.resolve(kwargs).delay(1100).then(getConnection);
    });
  }
}

let getChannel = function(kwargs) {
  let {state, config} = kwargs;
  if (config.channelIsCached !== false && state.channel) {
    debugx.enabled && debugx('getChannel() - channel has been available');
    return Promise.resolve(state.channel);
  }
  return getTicket(kwargs).then(function() {
    if (config.channelIsCached !== false && state.channel) {
      debugx.enabled && debugx('getChannel() - channel has been available');
      return Promise.resolve(state.channel);
    } else {
      debugx.enabled && debugx('getChannel() - make a new channel');
      return getConnection(kwargs).then(function(conn) {
        debugx.enabled && debugx('getChannel() - connection has already');
        let createChannel = Promise.promisify(conn.createConfirmChannel, {context: conn});
        return createChannel().then(function(ch) {
          ch.on('close', function() {
            debugx.enabled && debugx('getChannel() - channel is closed');
            delete state.channel;
          });
          ch.on('error', function(err) {
            debugx.enabled && debugx('getChannel() - channel has error');
            delete state.channel;
          });
          ch.on('drain', function() {
            debugx.enabled && debugx('getChannel() - channel has drained, open the valve');
            state.valve && state.valve.set(true);
          })
          debugx.enabled && debugx('getChannel() - channel is created successfully');
          return (state.channel = ch);
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
  let {state, config} = kwargs;
  if (state.exchangeAsserted) return Promise.resolve(state.exchangeAsserted);
  let ch_assertExchange = Promise.promisify(ch.assertExchange, {context: ch});
  return ch_assertExchange(config.exchangeName, config.exchangeType, {
    durable: config.durable,
    autoDelete: config.autoDelete
  }).then(function(eok) {
    return (state.exchangeAsserted = eok);
  });
}

let assertQueue = function(kwargs, ch, forced) {
  let {state, config} = kwargs;
  if (!forced && state.queueAsserted) return Promise.resolve(state.queueAsserted);
  return Promise.promisify(ch.assertQueue, {context: ch})(config.queueName, {
    durable: config.durable,
    exclusive: config.exclusive
  }).then(function(qok) {
    return (state.queueAsserted = qok);
  });
}

let checkQueue = function(kwargs, ch) {
  return Promise.promisify(ch.checkQueue, {context: ch})(kwargs.config.queueName);
}

let purgeQueue = function(kwargs, ch) {
  return Promise.promisify(ch.purgeQueue, {context: ch})(kwargs.config.queueName);
}

let sendToQueue = function(kwargs, data, opts) {
  let {state, config} = kwargs;
  opts = opts || {};
  debugx.enabled && debugx('%s() an object to rabbitmq queue', kwargs.name);
  return getChannel(kwargs).then(function(ch) {
    return assertQueue(kwargs, ch).then(function(qok) {
      let connection = state.connection;
      return new Promise(function(onResolved, onRejected) {
        let unexpectedClosing = function() {
          onRejected({ msg: 'Timeout exception' });
        }
        connection.on('error', unexpectedClosing);
        try {
          checkValve(kwargs, function() {
            let drained = ch.sendToQueue(config.queueName, zapper.bufferify(data), opts, function(err, ok) {
              connection.removeListener('error', unexpectedClosing);
              if (err) {
                onRejected(err);
              } else {
                ok = ok || {};
                ok.drained = true;
                onResolved(ok);
              }
            });
            if (!drained) {
              debugx.enabled && debugx('channel is overflowed, close the valve');
              state.valve && state.valve.set(false);
            }
          });
        } catch(exception) {
          connection.removeListener('error', unexpectedClosing);
          onRejected(exception);
        }
      });
    });
  }).catch(function(err) {
    debugx.enabled && debugx('sendToQueue() has failed, recall ...');
    state.connection = null;
    state.channel = null;
    return sendToQueue(kwargs, data, opts);
  });
}

let checkValve = function(ctx, whenConditionMet) {
  let {state} = ctx;
  if (state.valve) {
    state.valve.wait(
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
      let connection = ctx.state.connection;
      connection.on('error', unexpectedClosing);
      try {
        checkValve(ctx, function() {
          let drained = channel.publish(target.exchangeName, target.routingKey, zapper.bufferify(data), opts, function(err, ok) {
            connection.removeListener('error', unexpectedClosing);
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
            ctx.state.valve && ctx.state.valve.set(false);
          }
        });
      } catch(exception) {
        connection.removeListener('error', unexpectedClosing);
        debugx.enabled && debugx('sendToExchange() throw exception: %s', JSON.stringify(exception));
        onRejected(exception);
      }
    });
  }).catch(function(err) {
    debugx.enabled && debugx('produce() - sendToExchange() failed, recall ...');
    ctx.state.connection = null;
    ctx.state.channel = null;
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

Handler.prototype.ready = function() {
  let self = this;
  let sandbox = { config: self.config, state: getProducerState(self) };
  return getChannel(sandbox).then(function(ch) {
    let ok = assertExchange(sandbox, ch);
    if (hasInbox(self)) {
      ok = ok.then(function(eok) {
        return resolveInbox(self).then(function(ctx) {
          return assertQueue(ctx, ch).then(function(qok) {
            if (ctx.config.noBinding === true) return qok;
            return bindInbox(ch, qok, eok, self.config.routingKey);
          });
        });
      });
    }
    if (hasTrash(self)) {
      ok = ok.then(function(eok) {
        return resolveTrash(self).then(function(ctx) {
          return assertQueue(ctx, ch);
        });
      });
    }
    if (hasChain(self)) {
      ok = ok.then(function(eok) {
        return resolveChain(self).then(function(ctx) {
          return assertQueue(ctx, ch);
        });
      });
    }
    return ok;
  });
}

Handler.prototype.open = Handler.prototype.ready;
Handler.prototype.prepare = Handler.prototype.ready;

Handler.prototype.produce = function(data, opts, override) {
  let self = this;
  opts = opts || {};
  debugx.enabled && debugx('produce() an object to rabbitmq');
  return lockProducer(self).then(function(producerState) {
    let ctx = { config: self.config, state: producerState };
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
  let self = this;
  opts = opts || {};
  debugx.enabled && debugx('exhaust() data from a readable stream');
  if (!(stream instanceof Readable)) return Promise.reject({
    message: '[source] is not a readable stream'
  });
  return lockPipeline(self).then(function(pipelineState) {
    let ctx = { config: self.config, state: pipelineState };
    return retrieveExchange(ctx, override).then(function(ref) {
      let producerStream = new ProducerStream(ctx, opts, ref);
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

let getInboxState = function(self) {
  self = self || this;
  self.inboxState = self.inboxState || {};
  self.inboxState.valve = self.inboxState.valve || locks.createCondVariable(true);
  return self.inboxState;
}

let hasInbox = function(self) {
  self = self || this;
  return self.config && self.config.inbox && (self.config.inbox.enabled !== false);
}

let resolveInbox = function(self) {
  self = self || this;
  if (!hasInbox(self)) return Promise.reject({ message: 'The inbox is unavailable' });
  return Promise.resolve({ name: 'enqueueInbox', config: self.config.inbox, state: getInboxState(self) });
}

let enqueueInbox = function(self, data, opts) {
  return resolveInbox(self).then(function(ctx) {
    return sendToQueue(ctx, data, opts);
  });
}

let bindInbox = function(ch, qok, eok, routingKey) {
  let ch_bindQueue = Promise.promisify(ch.bindQueue, {context: ch});
  return ch_bindQueue(qok.queue, eok.exchange, routingKey, {}).then(function(ok) {
    debugx.enabled && debugx('bindInbox() - queue: %s has been bound to %s', qok.queue, eok.exchange);
    return qok;
  });
}

Handler.prototype.checkInbox = function() {
  return resolveInbox(this).then(function(ctx) {
    return getChannel(ctx).then(function(ch) {
      return checkQueue(ctx, ch);
    });
  });
}

Handler.prototype.purgeInbox = function() {
  return resolveInbox(this).then(function(ctx) {
    return getChannel(ctx).then(function(ch) {
      return assertQueue(ctx, ch).then(function() {
        return purgeQueue(ctx, ch);
      });
    });
  });
}

Handler.prototype.consume = function(callback) {
  let self = this;

  assert.ok(zapper.isFunction(callback), 'callback should be a function');

  let ctx = { config: self.config.inbox, state: getInboxState(self) };
  ctx.state.count = ctx.state.count || 0;

  debugx.enabled && debugx('consume() - subscribe the Inbox queue');
  return getChannel(ctx).then(function(ch) {

    if (ctx.config.prefetch && ctx.config.prefetch >= 0) {
      debugx.enabled && debugx('consume() - set channel prefetch: %s', ctx.config.prefetch);
      ch.prefetch(ctx.config.prefetch, true);
    }

    let ok = assertQueue(ctx, ch, true);

    ok = ok.then(function(qok) {
      debugx.enabled && debugx('consume() - queue info: %s', JSON.stringify(qok));
      if (ctx.config.maxSubscribers && ctx.config.maxSubscribers <= qok.consumerCount) {
        let error = {
          consumerCount: qok.consumerCount,
          maxSubscribers: ctx.config.maxSubscribers,
          message: 'exceeding quota limits of subscribers'
        }
        debugx.enabled && debugx('consume() - Error: %s', JSON.stringify(error));
        return Promise.reject(error);
      }
      return qok;
    });

    ok = ok.then(function(qok) {
      if (ctx.config.noBinding === true) return qok;
      return assertExchange({ config: self.config, state: ctx.state }, ch).then(function(eok) {
        let ch_bindQueue = Promise.promisify(ch.bindQueue, {context: ch});
        return ch_bindQueue(qok.queue, eok.exchange, self.config.routingKey, {}).then(function() {
          debugx.enabled && debugx('consume() - queue: %s has been bound', qok.queue);
          return qok;
        });
      });
    });

    ok = ok.then(function(qok) {
      let ch_consume = Promise.promisify(ch.consume, {context: ch});
      return ch_consume(qok.queue, function(msg) {
        ctx.state.count++;
        debug0.enabled && debug0('consume() - received message: %s, fields: %s, amount: %s', 
          msg.content, JSON.stringify(msg.fields), ctx.state.count);
        callback(msg.content, {
          fields: msg.fields,
          properties: msg.properties
        }, function done(err, msgz) {
          if (ctx.config.noAck !== true) {
            if (err) {
              let prom;
              let content = msg && msg.content && zapper.bufferify(msg.content) || null;
              let props = msg && msg.properties && lodash.clone(msg.properties) || {};
              let rcfg = self.config.trash;
              let headers = props.headers || {};
              headers[rcfg.redeliveredCountName] = (headers[rcfg.redeliveredCountName] || 0) + 1;
              if (headers[rcfg.redeliveredCountName] <= rcfg.redeliveredLimit) {
                debugx.enabled && debugx('consume() - enqueueInbox message');
                prom = enqueueInbox(self, content, props);
              } else {
                if (hasTrash(self)) {
                  debugx.enabled && debugx('consume() - enqueueTrash message');
                  prom = enqueueTrash(self, content, props);
                } else {
                  prom = Promise.resolve();
                }
              }
              prom.then(function() {
                ch.nack(msg, false, false);
              }).catch(function(err) {
                ch.nack(msg, false, true);
              });
            } else {
              if (hasChain(self)) {
                let content = msgz && msgz.content  && zapper.bufferify(msgz.content);
                content = content || msg && msg.content && zapper.bufferify(msg.content) || null;
                let props = msgz && msgz.properties && lodash.clone(msgz.properties);
                props = props || msg && msg.properties && lodash.clone(msg.properties) || {};
                enqueueChain(self, content, props).then(function() {
                  ch.ack(msg);
                }).catch(function(err) {
                  ch.nack(msg, false, true);
                });
              } else {
                ch.ack(msg);
              }
            }
          }
          ctx.state.count--;
        });
      }, {noAck: (ctx.config.noAck === true)});
    });

    return ok.then(function(result) {
      debugx.enabled && debugx('consume() - consumerTag: %s. CTRL+C to exit', result.consumerTag);
      ctx.state.consumerRefs = ctx.state.consumerRefs || [];
      ctx.state.consumerRefs.push(result);
      return result;
    });
  });
}

Handler.prototype.process = Handler.prototype.consume;

let getChainState = function(self) {
  self = self || this;
  self.chainState = self.chainState || {};
  self.chainState.valve = self.chainState.valve || locks.createCondVariable(true);
  return self.chainState;
}

let hasChain = function(self) {
  self = self || this;
  return self.config && self.config.chain && (self.config.chain.enabled !== false);
}

let resolveChain = function(self) {
  self = self || this;
  if (!hasChain(self)) return Promise.reject({ message: 'The chain is unavailable' });
  return Promise.resolve({ name: 'enqueueChain', config: self.config.chain, state: getChainState(self) });
}

let enqueueChain = function(self, data, opts) {
  return resolveChain(self).then(function(ctx) {
    return sendToQueue(ctx, data, opts);
  });
}

Handler.prototype.checkChain = function() {
  return resolveChain(this).then(function(ctx) {
    return getChannel(ctx).then(function(ch) {
      return checkQueue(ctx, ch);
    });
  });
}

Handler.prototype.purgeChain = function() {
  return resolveChain(this).then(function(ctx) {
    return getChannel(ctx).then(function(ch) {
      return assertQueue(ctx, ch).then(function() {
        return purgeQueue(ctx, ch);
      });
    });
  });
}

let getTrashState = function(self) {
  self = self || this;
  self.trashState = self.trashState || {};
  self.trashState.valve = self.trashState.valve || locks.createCondVariable(true);
  return self.trashState;
}

let hasTrash = function(self) {
  self = self || this;
  return self.config && self.config.trash && (self.config.trash.enabled !== false);
}

let resolveTrash = function(self) {
  self = self || this;
  if (!hasTrash(self)) return Promise.reject({ message: 'The trash is unavailable' });
  return Promise.resolve({ name: 'enqueueTrash', config: self.config.trash, state: getTrashState(self) });
}

let enqueueTrash = function(self, data, opts) {
  return resolveTrash(self).then(function(ctx) {
    return sendToQueue(ctx, data, opts);
  });
}

Handler.prototype.checkTrash = function() {
  return resolveTrash(this).then(function(ctx) {
    return getChannel(ctx).then(function(ch) {
      return checkQueue(ctx, ch);
    });
  });
}

Handler.prototype.purgeTrash = function() {
  return resolveTrash(this).then(function(ctx) {
    return getChannel(ctx).then(function(ch) {
      return assertQueue(ctx, ch).then(function() {
        return purgeQueue(ctx, ch);
      });
    });
  });
}

Handler.prototype.recycle = function(callback) {
  let self = this;

  assert.ok(zapper.isFunction(callback), 'callback should be a function');

  return resolveTrash(self).then(function(ctx) {
    ctx.state.count = ctx.state.count || 0;

    debugx.enabled && debugx('recycle() - get an object from Trash');
    return getChannel(ctx).then(function(ch) {

      if (ctx.config.prefetch && ctx.config.prefetch >= 0) {
        debugx.enabled && debugx('recycle() - set channel prefetch: %s', ctx.config.prefetch);
        ch.prefetch(ctx.config.prefetch, true);
      }

      let ok = assertQueue(ctx, ch, true);

      ok = ok.then(function(qok) {
        debugx.enabled && debugx('recycle() - queue info: %s', JSON.stringify(qok));
        if (ctx.config.maxSubscribers && ctx.config.maxSubscribers <= qok.consumerCount) {
          let error = {
            consumerCount: qok.consumerCount,
            maxSubscribers: ctx.config.maxSubscribers,
            message: 'exceeding quota limits of subscribers'
          }
          debugx.enabled && debugx('recycle() - Error: %s', JSON.stringify(error));
          return Promise.reject(error);
        }
        return qok;
      });

      ok = ok.then(function(qok) {
        return Promise.promisify(ch.consume, {context: ch})(qok.queue, function(msg) {
          ctx.state.count++;
          debugx.enabled && debugx('recycle() - received message: %s, fields: %s, amount: %s', 
            msg.content, JSON.stringify(msg.fields), ctx.state.count);
          callback(msg.content, {
            fields: msg.fields,
            properties: msg.properties
          }, function done(err) {
            if (ctx.config.noAck !== true) {
              err ? ch.nack(msg) :ch.ack(msg);
            }
            ctx.state.count--;
          });
        }, {noAck: (ctx.config.noAck === true)});
      });

      return ok.then(function(result) {
        debugx.enabled && debugx('recycle() - consumerTag: %s. CTRL+C to exit', result.consumerTag);
        ctx.state.consumerRefs = ctx.state.consumerRefs || [];
        ctx.state.consumerRefs.push(result);
        return result;
      });
    });
  });
}

let examineGarbage = function(self) {
  self = self || this;
  return resolveTrash(self).then(function(ctx) {
    if (ctx.state.garbage) return ctx.state.garbage;
    return getChannel(ctx).then(function(ch) {
      return assertQueue(ctx, ch).then(function(qok) {
        return Promise.promisify(ch.get, {context: ch})(qok.queue, {});
      }).then(function(msgOrFalse) {
        debugx.enabled && debugx('examineGarbage() - msg: %s', JSON.stringify(msgOrFalse));
        if (msgOrFalse !== false) ctx.state.garbage = msgOrFalse;
        return ctx.state.garbage;
      });
    });
  });
}

let garbageAction = {};

let discardGarbage = garbageAction['discard'] = function(self) {
  self = self || this;
  return resolveTrash(self).then(function(ctx) {
    if (!ctx.state.garbage) return false;
    return getChannel(ctx).then(function(ch) {
      debugx.enabled && debugx('discardGarbage() - nack()');
      ch.nack(ctx.state.garbage, false, false);
      ctx.state.garbage = undefined;
      return true;
    });
  });
}

let restoreGarbage = garbageAction['restore'] = function(self) {
  self = self || this;
  return resolveTrash(self).then(function(ctx) {
    if (!ctx.state.garbage) return false;
    return getChannel(ctx).then(function(ch) {
      debugx.enabled && debugx('restoreGarbage() - nack()');
      ch.nack(ctx.state.garbage);
      ctx.state.garbage = undefined;
      return true;
    });
  });
}

let recoverGarbage = garbageAction['recover'] = function(self) {
  self = self || this;
  return resolveTrash(self).then(function(ctx) {
    if (!ctx.state.garbage) return false;
    return getChannel(ctx).then(function(ch) {
      let msg = ctx.state.garbage;
      return enqueueInbox(self, msg.content.toString(), msg.properties).then(function(result) {
        debugx.enabled && debugx('recoverGarbage() - ack()');
        ch.ack(ctx.state.garbage);
        ctx.state.garbage = undefined;
        return true;
      });
    });
  });
}

let requeueGarbage = garbageAction['requeue'] = function(self) {
  self = self || this;
  return resolveTrash(self).then(function(ctx) {
    if (!ctx.state.garbage) return false;
    return getChannel(ctx).then(function(ch) {
      let msg = ctx.state.garbage;
      return enqueueTrash(self, msg.content.toString(), msg.properties).then(function(result) {
        debugx.enabled && debugx('requeueGarbage() - ack()');
        ch.ack(ctx.state.garbage);
        ctx.state.garbage = undefined;
        return true;
      });
    });
  });
}

Handler.prototype.examine = function(callback) {
  let self = this;
  let result = { obtained: 0 };
  result.callback = zapper.isFunction(callback);
  return examineGarbage(self).then(function(msg) {
    if (msg) {
      result.obtained = 1;
      if (!result.callback) return result;
      return new Promise(function(resolved, rejected) {
        let copied = lodash.pick(msg, ['content', 'fields', 'properties']);
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

Handler.prototype.close = function() {
  let self = this;
  return Promise.resolve().then(function() {
    return stopPublisher({state: getProducerState(self) });
  }).then(function() {
    return stopPublisher({state: getPipelineState(self) });
  }).then(function() {
    return stopSubscriber({state: getInboxState(self) });
  }).then(function() {
    return stopSubscriber({state: getTrashState(self) });
  }).then(function() {
    return stopSubscriber({state: getChainState(self) });
  });
}

Handler.prototype.destroy = Handler.prototype.close;

let stopPublisher = function(self) {
  self = self || this;
  return Promise.resolve().then(function() {
    if (self.state.channel) {
      let ch_close = Promise.promisify(self.state.channel.close, {
        context: self.state.channel
      });
      return ch_close().then(function(ok) {
        delete self.state.channel;
        debugx.enabled && debugx('destroy() - channel is closed: %s', JSON.stringify(ok));
        return true;
      });
    }
    return true;
  }).then(function() {
    if (self.state.connection) {
      let ch_close = Promise.promisify(self.state.connection.close, {
        context: self.state.connection
      });
      return ch_close().then(function(ok) {
        delete self.state.connection;
        debugx.enabled && debugx('destroy() - connection is closed: %s', JSON.stringify(ok));
        return true;
      });
    }
    return true;
  });
}

let stopSubscriber = function(self) {
  self = self || this;
  return Promise.resolve().then(function() {
    if (self.state.channel && self.state.consumerRefs && self.state.consumerRefs.length > 0) {
      debugx.enabled && debugx('stopSubscriber() - consumers cancellation has been invoked');
      let ch_cancel = Promise.promisify(self.state.channel.cancel, {
        context: self.state.channel
      });
      return Promise.mapSeries(self.state.consumerRefs, function(consumerRef) {
        return ch_cancel(consumerRef.consumerTag).then(function(ok) {
          debugx.enabled && debugx('stopSubscriber() - consumer is cancelled: %s', JSON.stringify(ok));
          return ok;
        });
      });
    }
    return true;
  }).then(function() {
    if (self.state.channel) {
      let ch_close = Promise.promisify(self.state.channel.close, {
        context: self.state.channel
      });
      return ch_close().then(function(ok) {
        delete self.state.channel;
        debugx.enabled && debugx('stopSubscriber() - channel is closed: %s', JSON.stringify(ok));
        return true;
      });
    }
    return true;
  }).then(function() {
    if (self.state.connection) {
      let ch_close = Promise.promisify(self.state.connection.close, {
        context: self.state.connection
      });
      return ch_close().then(function(ok) {
        delete self.state.connection;
        debugx.enabled && debugx('stopSubscriber() - connection is closed: %s', JSON.stringify(ok));
        return true;
      });
    }
    return true;
  });
}

module.exports = Handler;
