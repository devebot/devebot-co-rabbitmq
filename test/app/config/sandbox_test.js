module.exports = {
  bridges: {
    rabbitmqWrapper: {
      rabbitmq: {
        amqplib: {
          host: 'amqp://master:zaq123edcx@192.168.56.56',
          exchangeType: 'direct',
          exchange: 'sample-exchange',
          routingKey: 'sample',
          queue: 'sample-queue',
          durable: true,
          noAck: false,
          prefetch: 2
        }
      }
    },
    rabbitmqExporter: {
      rabbitmq: {
        amqplib: {
          host: 'amqp://master:zaq123edcx@192.168.56.56',
          exchangeType: 'direct',
          exchange: 'sample-export-exchange',
          routingKey: 'sample-export',
          queue: 'sample-export-queue',
          durable: true,
          noAck: false,
          prefetch: 10,
          recycler: {
            retryCount: 3,
            queueName: 'sample-garbage-queue',
            durable: true,
            noAck: false,
            prefetch: 10
          }
        }
      }
    }
  }
};
