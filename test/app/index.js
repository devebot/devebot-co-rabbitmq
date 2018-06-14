'use strict';

var path = require('path');

var app = require('devebot').launchApplication({
  appRootPath: __dirname
}, [], [
  {
    name: 'devebot-co-rabbitmq',
    path: path.join(__dirname, '../../index.js'),
    presets: {
      schemaValidation: true
    }
  }
]);

if (require.main === module) app.server.start();

module.exports = app;
