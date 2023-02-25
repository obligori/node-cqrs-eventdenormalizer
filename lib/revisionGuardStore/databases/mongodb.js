'use strict';

const debug = require('debug')('denormalizer:revisionGuardStore:mongodb');

const _ = require('lodash');

const Store = require('#/revisionGuardStore/base');
const ConcurrencyError = require('#/errors/concurrencyError');


const mongo = Store.use('mongodb');
const mongoVersion = Store.use('mongodb/package.json').version;

const isNew = mongoVersion.indexOf('1.') !== 0;

const ObjectID = isNew ? mongo.ObjectID : mongo.BSONPure.ObjectID;


class Mongo extends Store {
  constructor (options) {
    super(options);

    const defaults = {
      host: 'localhost',
      port: 27017,
      dbName: 'readmodel',
      collectionName: 'revision', // ,
      // heartbeat: 60 * 1000
    };

    _.defaults(options, defaults);

    const defaultOpt = {
      ssl: false,
    };

    options.options = options.options || {};

    if (isNew) {
      defaultOpt.autoReconnect = false;
      defaultOpt.useNewUrlParser = true;
      defaultOpt.useUnifiedTopology = true;
      _.defaults(options.options, defaultOpt);
    }
    else {
      defaultOpt.auto_reconnect = false;
      _.defaults(options.options, defaultOpt);
    }

    this.options = options;
  }

  connect (callback) {
    const self = this;

    const options = this.options;

    let connectionUrl;

    if (options.url) {
      connectionUrl = options.url;
    }
    else {
      const members = options.servers ?
        options.servers :
        [{host: options.host, port: options.port}];

      const memberString = _(members).map(function (m) {
        return m.host + ':' + m.port;
      });
      const authString = options.username && options.password ?
        options.username + ':' + options.password + '@' :
        '';
      const optionsString = options.authSource ?
        '?authSource=' + options.authSource :
        '';

      connectionUrl = 'mongodb://' + authString + memberString + '/' + options.dbName + optionsString;
    }

    let client;

    if (mongo.MongoClient.length === 2) {
      client = new mongo.MongoClient(connectionUrl, options.options);
      client.connect(function (err, cl) {
        if (err) {
          debug(err);
          if (callback) callback(err);
          return;
        }

        self.db = cl.db(cl.s.options.dbName);
        if (!self.db.close) {
          self.db.close = cl.close.bind(cl);
        }
        initDb();
      });
    }
    else {
      client = new mongo.MongoClient();
      client.connect(connectionUrl, options.options, function (err, db) {
        if (err) {
          debug(err);
          if (callback) callback(err);
          return;
        }

        self.db = db;
        initDb();
      });
    }

    function initDb () {
      self.db.on('close', function () {
        self.emit('disconnect');
        self.stopHeartbeat();
      });

      const finish = function (err) {
        self.store = self.db.collection(options.collectionName);
        //        self.store.ensureIndex({ 'aggregateId': 1, date: 1 }, function() {});
        if (!err) {
          self.emit('connect');

          if (self.options.heartbeat) {
            self.startHeartbeat();
          }
        }
        if (callback) callback(err, self);
      };

      finish();
    }
  }

  stopHeartbeat () {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      delete this.heartbeatInterval;
    }
  }

  startHeartbeat () {
    const self = this;

    const gracePeriod = Math.round(this.options.heartbeat / 2);
    this.heartbeatInterval = setInterval(function () {
      const graceTimer = setTimeout(function () {
        if (self.heartbeatInterval) {
          console.error((new Error('Heartbeat timeouted after ' + gracePeriod + 'ms (mongodb)')).stack);
          self.disconnect();
        }
      }, gracePeriod);

      self.db.command({ping: 1}, function (err) {
        if (graceTimer) clearTimeout(graceTimer);
        if (err) {
          console.error(err.stack || err);
          self.disconnect();
        }
      });
    }, this.options.heartbeat);
  }

  disconnect (callback) {
    this.stopHeartbeat();

    if (!this.db) {
      if (callback) callback(null);
      return;
    }

    this.db.close(callback || function () {});
  }

  getNewId (callback) {
    callback(null, new ObjectID().toString());
  }

  get (id, callback) {
    if (!id || !_.isString(id)) {
      const err = new Error('Please pass a valid id!');
      debug(err);
      return callback(err);
    }

    this.store.findOne({_id: id}, function (err, entry) {
      if (err) {
        return callback(err);
      }

      if (!entry) {
        return callback(null, null);
      }

      callback(null, entry.revision || null);
    });
  }

  set (id, revision, oldRevision, callback) {
    if (!id || !_.isString(id)) {
      var err = new Error('Please pass a valid id!');
      debug(err);
      return callback(err);
    }
    if (!revision || !_.isNumber(revision)) {
      var err = new Error('Please pass a valid revision!');
      debug(err);
      return callback(err);
    }

    this.store.update({_id: id, revision: oldRevision}, {_id: id, revision: revision}, {safe: true, upsert: true}, function (err, modifiedCount) {
      if (isNew) {
        if (modifiedCount && modifiedCount.result && modifiedCount.result.n === 0) {
          err = new ConcurrencyError();
          debug(err);
          if (callback) {
            callback(err);
          }
          return;
        }
      }
      else {
        if (modifiedCount === 0) {
          err = new ConcurrencyError();
          debug(err);
          if (callback) {
            callback(err);
          }
          return;
        }
      }
      if (err && err.message && err.message.match(/duplicate key/i)) {
        debug(err);
        err = new ConcurrencyError();
        debug(err);
        if (callback) {
          callback(err);
        }
        return;
      }
      if (callback) {
        callback(err);
      }
    });
  }

  saveLastEvent (evt, callback) {
    this.store.save({_id: 'THE_LAST_SEEN_EVENT', event: evt}, {safe: true}, function (err) {
      if (callback) {
        callback(err);
      }
    });
  }

  getLastEvent (callback) {
    this.store.findOne({_id: 'THE_LAST_SEEN_EVENT'}, function (err, entry) {
      if (err) {
        return callback(err);
      }

      if (!entry) {
        return callback(null, null);
      }

      callback(null, entry.event || null);
    });
  }

  clear (callback) {
    this.store.remove({}, {safe: true}, callback);
  }
}


module.exports = Mongo;
