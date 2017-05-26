#!/usr/bin/env node

var amqp = require('amqplib/callback_api');

amqp.connect('amqp://master:zaq123edcx@192.168.56.56', function(err, conn) {
  conn.createChannel(function(err, ch) {
    ch.assertExchange('sample-exchange', 'direct', {
      durable: true
    });

    for(var i=0; i<1000; i++) {
      ch.publish('sample-exchange', 'sample', new Buffer(JSON.stringify({ code: i, msg: 'Hello world' })));
    }

  console.log(" [x] Sent 'Hello World!'");
    ch.close(function(err) {
      console.log(" [x] Channel has been closed");
      conn.close(function(err) {
        console.log(" [x] Connection has been closed");
      });
    });
  });
});