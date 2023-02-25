'use strict';

const tolerate = require('tolerance');
const _ = require('lodash');
const fs = require('fs');
const path = require('path');

const Base = require('denormalizer/lib/revisionGuardStore/base');

function exists (toCheck) {
  let _exists = fs.existsSync || path.existsSync;
  if (fs.accessSync) {
    _exists = function (toCheck) {
      try {
        fs.accessSync(toCheck);
        return true;
      }
      catch (e) {
        return false;
      }
    };
  }
  return _exists(toCheck);
}

function getSpecificDbImplementation (options) {
  options = options || {};

  options.type = options.type || 'inmemory';

  if (_.isFunction(options.type)) {
    return options.type;
  }

  options.type = options.type.toLowerCase();

  const dbPath = __dirname + '/databases/' + options.type + '.js';

  if (!exists(dbPath)) {
    const errMsg = 'Implementation for db "' + options.type + '" does not exist!';
    console.log(errMsg);
    throw new Error(errMsg);
  }

  try {
    const db = require(dbPath);
    return db;
  }
  catch (err) {
    if (err.message.indexOf('Cannot find module') >= 0 &&
      err.message.indexOf('\'') > 0 &&
      err.message.lastIndexOf('\'') !== err.message.indexOf('\'')) {
      const moduleName = err.message.substring(err.message.indexOf('\'') + 1, err.message.lastIndexOf('\''));
      console.log('Please install module "' + moduleName +
        '" to work with db implementation "' + options.type + '"!');
    }

    throw err;
  }
}

module.exports = {
  Store: Base,

  create: function (options, callback) {
    if (typeof options === 'function') {
      callback = options;
      options = {};
    }

    options = options || {};

    let Store;

    try {
      Store = getSpecificDbImplementation(options);
    }
    catch (err) {
      if (callback) callback(err);
      throw err;
    }

    const store = new Store(options);
    if (callback) {
      process.nextTick(function () {
        tolerate(function (callback) {
          store.connect(callback);
        }, options.timeout || 0, callback || function () {
        });
      });
    }
    return store;
  },
};
