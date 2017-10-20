module.exports = {
  bridges: {
    rabbitmqWrapper: {
      rabbitmq: {
        amqplib: {
          host: process.env.OPFLOW_TDD_URI || 'amqp://master:zaq123edcx@opflow-broker-default?frameMax=0x1000',
          exchangeType: 'direct',
          exchange: 'sample-exchange',
          routingKey: 'sample',
          queue: 'sample-queue',
          durable: true,
          noAck: false,
          prefetch: 2,
          recycler: {
            queue: 'sample-trash',
            durable: true,
            noAck: false,
            prefetch: 1,
            redeliveredCountName: 'x-redelivered-count',
            redeliveredLimit: 3,
          }
        }
      }
    },
    rabbitmqExporter: {
      rabbitmq: {
        amqplib: {
          host: process.env.OPFLOW_TDD_URI || 'amqp://master:zaq123edcx@opflow-broker-default?frameMax=0x1000',
          exchangeType: 'direct',
          exchange: 'sample-export-exchange',
          routingKey: 'sample-export',
          queue: 'sample-export-queue',
          durable: true,
          noAck: false,
          prefetch: 10,
          recycler: {
            queue: 'sample-export-trash',
            durable: true,
            noAck: false,
            prefetch: 10
          }
        }
      }
    }
  }
};
