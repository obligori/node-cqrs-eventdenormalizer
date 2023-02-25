'use strict';

const debug = require('debug')('denormalizer:revisionGuardStore:inmemory');

const _ = require('lodash');

const Store = require('lib/revisionGuardStore/base');
const ConcurrencyError = require('lib/errors/concurrencyError');


class InMemory extends Store {
  constructor (options) {
    super(options);
    this.store = {};
    this.lastEvent = null;
  }

  connect (callback) {
    this.emit('connect');
    if (callback) callback(null, this);
  }

  disconnect (callback) {
    this.emit('disconnect');
    if (callback) callback(null);
  }

  get (id, callback) {
    if (!id || !_.isString(id)) {
      const err = new Error('Please pass a valid id!');
      debug(err);
      return callback(err);
    }

    callback(null, this.store[id] || null);
  }

  set (id, revision, oldRevision, callback) {
    if (!id || !_.isString(id)) {
      const err = new Error('Please pass a valid id!');
      debug(err);
      return callback(err);
    }
    if (!revision || !_.isNumber(revision)) {
      const err = new Error('Please pass a valid revision!');
      debug(err);
      return callback(err);
    }

    if (this.store[id] && this.store[id] !== oldRevision) {
      return callback(new ConcurrencyError());
    }

    this.store[id] = revision;

    callback(null);
  }

  saveLastEvent (evt, callback) {
    this.lastEvent = evt;
    if (callback) callback(null);
  }

  getLastEvent (callback) {
    callback(null, this.lastEvent);
  }

  clear (callback) {
    this.store = {};
    this.lastEvent = null;
    if (callback) callback(null);
  }
}

module.exports = InMemory;
