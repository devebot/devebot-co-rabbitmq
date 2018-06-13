module.exports = {
  bridges: {
    rabbitmq: {
      application: {
        rabbitmqWrapper: {
          amqplib: {
            host: process.env.OPFLOW_TDD_URI || 'amqp://master:zaq123edcx@opflow-broker-default?frameMax=0x1000',
            exchangeType: 'direct',
            exchange: 'sample-exchange',
            routingKey: 'sample',
            inbox: {
              queueName: 'sample-queue',
              durable: true,
              noAck: false,
              prefetch: 2,
              messageTtl: 5000
            },
            trash: {
              queueName: 'sample-trash',
              durable: true,
              noAck: false,
              prefetch: 1,
              redeliveredCountName: 'x-redelivered-count',
              redeliveredLimit: 3,
            }
          }
        },
        rabbitmqExporter: {
          amqplib: {
            host: process.env.OPFLOW_TDD_URI || 'amqp://master:zaq123edcx@opflow-broker-default?frameMax=0x1000',
            exchangeType: 'direct',
            exchange: 'sample-export-exchange',
            routingKey: 'sample-export',
            inbox: {
              queueName: 'sample-export-queue',
              durable: true,
              noAck: false,
              prefetch: 10
            },
            trash: {
              queueName: 'sample-export-trash',
              durable: true,
              noAck: false,
              prefetch: 10
            }
          }
        }
      }
    }
  }
};
