'use strict';

const debug = require('debug')('denormalizer:eventExtender');

const _ = require('lodash');
const dotty = require('dotty');

const Definition = require('lib/definitionBase');

/**
 * EventExtender constructor
 * @param {Object}             meta     Meta infos like: { name: 'name', version: 1, payload: 'some.path' }
 * @param {Function || String} evtExtFn Function handle
 *                                      `function(evt, col, callback){}`
 * @constructor
 */
class EventExtender extends Definition {
  constructor (meta, evtExtFn) {
    super(meta);
    meta = meta || {};

    if (!evtExtFn || !(_.isFunction(evtExtFn))) {
      const err = new Error('extender function not injected!');
      debug(err);
      throw err;
    }

    this.version = meta.version || 0;
    this.aggregate = meta.aggregate || null;
    this.context = meta.context || null;
    this.payload = meta.payload || null;
    this.id = meta.id || null;

    this.evtExtFn = evtExtFn;
  }


  /**
   * Injects the needed collection.
   * @param {Object} collection The collection object to inject.
   */
  useCollection (collection) {
    if (!collection || !_.isObject(collection)) {
      const err = new Error('Please pass a valid collection!');
      debug(err);
      throw err;
    }

    this.collection = collection;
  }

  /**
   * Loads the appropriate viewmodel by id.
   * @param {String}   id       The viewmodel id.
   * @param {Function} callback The function that will be called when this action has finished
   *                            `function(err, vm){}`
   */
  loadViewModel (id, callback) {
    this.collection.loadViewModel(id, callback);
  }

  /**
   * Loads a viewModel array by optional query and query options.
   * @param {Object}   query        The query to find the viewModels. (mongodb style) [optional]
   * @param {Object}   queryOptions The query options. (mongodb style) [optional]
   * @param {Function} callback     The function, that will be called when the this action is completed.
   *                                `function(err, vms){}` vms is of type Array.
   */
  findViewModels (query, queryOptions, callback) {
    if (typeof query === 'function') {
      callback = query;
      query = {};
      queryOptions = {};
    }
    if (typeof queryOptions === 'function') {
      callback = queryOptions;
      queryOptions = {};
    }

    this.collection.findViewModels(query, queryOptions, callback);
  }

  /**
   * Extracts the id from the event or generates a new one.
   * @param {Object}   evt      The event object.
   * @param {Function} callback The function that will be called when this action has finished
   *                            `function(err, id){}`
   */
  extractId (evt, callback) {
    if (this.id && dotty.exists(evt, this.id)) {
      debug('found viewmodel id in event');
      return callback(null, dotty.get(evt, this.id));
    }

    if (this.getNewIdForThisEventExtender) {
      debug('[' + this.name + '] found eventextender id getter in event');
      return this.getNewIdForThisEventExtender(evt, callback);
    }

    debug('not found viewmodel id in event, generate new id');
    this.collection.getNewId(callback);
  }

  /**
   * Extends the event.
   * @param {Object}   evt      The event object.
   * @param {Function} callback The function that will be called when this action has finished
   *                            `function(err, extendedEvent){}`
   */
  extend (evt, callback) {
    const self = this;
    let payload = evt;

    if (self.payload && self.payload !== '') {
      payload = dotty.get(evt, self.payload);
    }

    if (self.evtExtFn.length === 3) {
      if (self.id) {
        self.extractId(evt, function (err, id) {
          if (err) {
            debug(err);
            return callback(err);
          }

          self.loadViewModel(id, function (err, vm) {
            if (err) {
              debug(err);
              return callback(err);
            }

            try {
              self.evtExtFn(_.cloneDeep(payload), vm, function () {
                try {
                  callback.apply(this, _.toArray(arguments));
                }
                catch (e) {
                  debug(e);
                  process.emit('uncaughtException', e);
                }
              });
            }
            catch (e) {
              debug(e);
              process.emit('uncaughtException', e);
            }
          });
        });
        return;
      }

      try {
        self.evtExtFn(_.cloneDeep(payload), self.collection, function () {
          try {
            callback.apply(this, _.toArray(arguments));
          }
          catch (e) {
            debug(e);
            process.emit('uncaughtException', e);
          }
        });
      }
      catch (e) {
        debug(e);
        process.emit('uncaughtException', e);
      }
      return;
    }

    if (self.evtExtFn.length === 1) {
      try {
        const res = self.evtExtFn(evt);
        try {
          callback(null, res);
        }
        catch (e) {
          debug(e);
          process.emit('uncaughtException', e);
        }
      }
      catch (e) {
        debug(e);
        process.emit('uncaughtException', e);
      }
      return;
    }

    if (self.evtExtFn.length === 2) {
      if (!self.collection || !self.id) {
        try {
          self.evtExtFn(evt, function () {
            try {
              callback.apply(this, _.toArray(arguments));
            }
            catch (e) {
              debug(e);
              process.emit('uncaughtException', e);
            }
          });
        }
        catch (e) {
          debug(e);
          process.emit('uncaughtException', e);
        }
        return;
      }

      self.extractId(evt, function (err, id) {
        if (err) {
          debug(err);
          return callback(err);
        }

        self.loadViewModel(id, function (err, vm) {
          if (err) {
            debug(err);
            return callback(err);
          }

          try {
            const res = self.evtExtFn(_.cloneDeep(payload), vm);
            try {
              callback(null, res);
            }
            catch (e) {
              debug(e);
              process.emit('uncaughtException', e);
            }
          }
          catch (e) {
            debug(e);
            process.emit('uncaughtException', e);
          }
        });
      });
    }
  }

  /**
   * Inject idGenerator for eventextender function if no id found.
   * @param   {Function}  fn      The function to be injected.
   * @returns {EventExtender} to be able to chain...
   */
  useAsId (fn) {
    if (!fn || !_.isFunction(fn)) {
      const err = new Error('Please pass a valid function!');
      debug(err);
      throw err;
    }


    if (fn.length === 2) {
      this.getNewIdForThisEventExtender = fn;
      return this;
    }

    this.getNewIdForThisEventExtender = function (evt, callback) {
      callback(null, fn(evt));
    };

    return this;
  }
}


module.exports = EventExtender;
