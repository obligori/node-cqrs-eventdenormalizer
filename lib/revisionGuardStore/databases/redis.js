'use strict';

const debug = require('debug')('denormalizer:revisionGuardStore:redis');

const _ = require('lodash');
const async = require('async');
const jsondate = require('jsondate');

const Store = require('#/revisionGuardStore/base');
const ConcurrencyError = require('#/errors/concurrencyError');


const redis = Store.use('redis');


class Redis extends Store {
  constructor (options) {
    super(options);

    const defaults = {
      host: 'localhost',
      port: 6379,
      prefix: 'readmodel_revision',
      retry_strategy: function (options) {
        return undefined;
      }, // ,
      // heartbeat: 60 * 1000
    };

    _.defaults(options, defaults);

    if (options.url) {
      const url = require('url').parse(options.url);
      if (url.protocol === 'redis:') {
        if (url.auth) {
          const userparts = url.auth.split(':');
          options.user = userparts[0];
          if (userparts.length === 2) {
            options.password = userparts[1];
          }
        }
        options.host = url.hostname;
        options.port = url.port;
        if (url.pathname) {
          options.db = url.pathname.replace('/', '', 1);
        }
      }
    }

    this.options = options;
  }

  connect (callback) {
    const self = this;

    const options = this.options;

    this.client = new redis.createClient(options.port || options.socket, options.host, _.omit(options, 'prefix'));

    this.prefix = options.prefix;

    let calledBack = false;

    if (options.password) {
      this.client.auth(options.password, function (err) {
        if (err && !calledBack && callback) {
          calledBack = true;
          if (callback) callback(err, self);
          return;
        }
        if (err) throw err;
      });
    }

    if (options.db) {
      this.client.select(options.db);
    }

    this.client.on('end', function () {
      self.disconnect();
      self.stopHeartbeat();
    });

    this.client.on('error', function (err) {
      console.log(err);

      if (calledBack) return;
      calledBack = true;
      if (callback) callback(null, self);
    });

    this.client.on('connect', function () {
      if (options.db) {
        self.client.send_anyways = true;
        self.client.select(options.db);
        self.client.send_anyways = false;
      }

      self.emit('connect');

      if (self.options.heartbeat) {
        self.startHeartbeat();
      }

      if (calledBack) return;
      calledBack = true;
      if (callback) callback(null, self);
    });
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
          console.error((new Error('Heartbeat timeouted after ' + gracePeriod + 'ms (redis)')).stack);
          self.disconnect();
        }
      }, gracePeriod);

      self.client.ping(function (err) {
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

    if (this.client) {
      this.client.end(true);
    }
    this.emit('disconnect');
    if (callback) callback(null, this);
  }

  getNewId (callback) {
    this.client.incr('nextItemId:' + this.prefix, function (err, id) {
      if (err) {
        return callback(err);
      }
      callback(null, id.toString());
    });
  }

  get (id, callback) {
    if (!id || !_.isString(id)) {
      const err = new Error('Please pass a valid id!');
      debug(err);
      return callback(err);
    }

    this.client.get(this.options.prefix + ':' + id, function (err, entry) {
      if (err) {
        return callback(err);
      }

      if (!entry) {
        return callback(null, null);
      }

      try {
        entry = jsondate.parse(entry.toString());
      }
      catch (error) {
        if (callback) callback(error);
        return;
      }

      callback(null, entry.revision || null);
    });
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

    const key = this.options.prefix + ':' + id;

    const self = this;

    this.client.watch(key, function (err) {
      if (err) {
        return callback(err);
      }

      self.get(id, function (err, rev) {
        if (err) {
          debug(err);
          if (callback) callback(err);
          return;
        }

        if (rev && rev !== oldRevision) {
          self.client.unwatch(function (err) {
            if (err) {
              debug(err);
            }

            err = new ConcurrencyError();
            debug(err);
            if (callback) {
              callback(err);
            }
          });
          return;
        }

        self.client.multi([['set'].concat([key, JSON.stringify({revision: revision})])]).exec(function (err, replies) {
          if (err) {
            debug(err);
            if (callback) {
              callback(err);
            }
            return;
          }
          if (!replies || replies.length === 0 || _.find(replies, function (r) {
            return (r !== 'OK' && r !== 1);
          })) {
            const err = new ConcurrencyError();
            debug(err);
            if (callback) {
              callback(err);
            }
            return;
          }
          if (callback) {
            callback(null);
          }
        });
      });
    });
  }

  saveLastEvent (evt, callback) {
    const key = this.options.prefix + ':THE_LAST_SEEN_EVENT';

    this.client.set(key, JSON.stringify({event: evt}), function (err) {
      if (callback) {
        callback(err);
      }
    });
  }

  getLastEvent (callback) {
    this.client.get(this.options.prefix + ':THE_LAST_SEEN_EVENT', function (err, entry) {
      if (err) {
        return callback(err);
      }

      if (!entry) {
        return callback(null, null);
      }

      try {
        entry = jsondate.parse(entry.toString());
      }
      catch (error) {
        if (callback) callback(error);
        return;
      }

      callback(null, entry.event || null);
    });
  }

  clear (callback) {
    const self = this;
    async.parallel([
      function (callback) {
        self.client.del('nextItemId:' + self.options.prefix, callback);
      },
      function (callback) {
        self.client.keys(self.options.prefix + ':*', function (err, keys) {
          if (err) {
            return callback(err);
          }
          async.each(keys, function (key, callback) {
            self.client.del(key, callback);
          }, callback);
        });
      },
    ], function (err) {
      if (err) {
        debug(err);
      }
      if (callback) callback(err);
    });
  }
}

module.exports = Redis;
