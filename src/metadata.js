'use strict';

const Devebot = require('devebot');
const lodash = Devebot.require('lodash');

let commonQueueSchema = {
  "type": "object",
  "properties": {
    "enabled": {
      "type": "boolean"
    },
    "uri": {
      "type": "string"
    },
    "queueName": {
      "type": "string"
    },
    "durable": {
      "type": "boolean"
    },
    "exclusive": {
      "type": "boolean"
    },
    "autoDelete": {
      "type": "boolean"
    },
    "maxListeners": {
      "type": "number"
    },
    "messageTtl": {
      "type": "number"
    },
    "expires": {
      "type": "number"
    },
    "maxLength": {
      "type": "number"
    },
    "maxPriority": {
      "type": "number"
    }
  }
}

let inboxSchema = lodash.merge({}, commonQueueSchema, {
  "properties": {
    "noAck": {
      "type": "boolean"
    },
    "noBinding": {
      "type": "boolean"
    },
    "prefetch": {
      "type": "number"
    },
    "maxSubscribers": {
      "type": "number"
    }
  }
});

let chainSchema = lodash.merge({}, commonQueueSchema, {
});

let trashSchema = lodash.merge({}, commonQueueSchema, {
  "properties": {
    "noAck": {
      "type": "boolean"
    },
    "prefetch": {
      "type": "number"
    },
    "maxSubscribers": {
      "type": "number"
    },
    "redeliveredCountName": {
      "type": "string"
    },
    "redeliveredLimit": {
      "type": "number"
    }
  }
});

module.exports = {
  schema: {
    "type": "object",
    "properties": {
      "amqplib": {
        "type": "object",
        "properties": {
          "enabled": {
            "type": "boolean"
          },
          "uri": {
            "type": "string"
          },
          "exchange": {
            "type": "string"
          },
          "exchangeType": {
            "type": "string"
          },
          "exchangeQuota": {
            "type": "number"
          },
          "exchangeMutex": {
            "type": "boolean"
          },
          "durable": {
            "type": "boolean"
          },
          "autoDelete": {
            "type": "boolean"
          },
          "alternateExchange": {
            "type": "string"
          },
          "routingKey": {
            "type": "string"
          },
          "inbox": {
            "$ref": "#/definitions/inbox"
          },
          "chain": {
            "$ref": "#/definitions/chain"
          },
          "trash": {
            "$ref": "#/definitions/trash"
          }
        }
      }
    },
    "required": ["amqplib"],
    "definitions": {
      "inbox": inboxSchema,
      "chain": chainSchema,
      "trash": trashSchema
    }
  }
}
