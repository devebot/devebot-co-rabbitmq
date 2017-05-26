'use strict';

var amqplib = require('amqplib');

var consume = function(callback, config) {
  console.log(' - consume() a message from rabbitmq');
  return amqplib.connect(config.host).then(function(conn) {
    return conn.createChannel().then(function(ch) {
      var ok = ch.assertExchange(config.exchange, config.exchangeType, {
        durable: config.durable
      });
      ok = ok.then(function() {
        return ch.assertQueue(config.queue, {
          durable: config.durable,
          exclusive: config.exclusive
        });
      });
      ok = ok.then(function(qok) {
        return ch.bindQueue(qok.queue, config.exchange, config.routingKey).then(function() {
          console.log(' - queue: %s has been bound', qok.queue);
          return qok.queue;
        });
      });
      ok = ok.then(function(queue) {
        return ch.consume(queue, function(msg) {
          console.log(' - received message: %s', JSON.stringify(msg));
          if (callback && typeof(callback) === 'function') {
            callback(msg.content, { fields: msg.fields, properties: msg.properties }, function done(err) {
            	if (config.noAck === false) {
            		err ? ch.nack(msg) :ch.ack(msg);
            	}
            });
          } else {
            console.log(' - callback (observer) is not available');
          }
        }, {noAck: config.noAck});
      });
      return ok.then(function() {
        console.log('[*] Waiting for logs. To exit press CTRL+C');
        return {};
      });
    });
  });
}

consume(function(message, info, done) {
	console.log('===== Message: %s', JSON.stringify(message));
	done({});
}, {
	host: 'amqp://master:zaq123edcx@192.168.56.56',
	exchangeType: 'direct',
	exchange: 'sample-exchange',
	routingKey: 'sample',
	queue: 'sample-queue',
	durable: true,
	noAck: false
});