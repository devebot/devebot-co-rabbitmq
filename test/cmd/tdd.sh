#!/bin/bash

pm2 start test/app/process.json;
node_modules/.bin/mocha test/tdd/*-test.js;
pm2 delete test/app/process.json;
true