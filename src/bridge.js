'use strict';

const Devebot = require('devebot');
const lodash = Devebot.require('lodash');
const Handler = require('./handler');

function Service(params = {}) {
  var handler = null;

  this.open = function(opts) {
    return (handler = handler || new Handler(lodash.assign(
      lodash.pick(params, ['logger', 'tracer']), params.amqplib, opts || {})));
  }
};

module.exports = Service;

Service.devebotMetadata = require('./metadata');
