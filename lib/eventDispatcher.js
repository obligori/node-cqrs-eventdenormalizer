'use strict';

const debug = require('debug')('denormalizer:eventDispatcher');

const _ = require('lodash');
const async = require('async');
const dotty = require('dotty');


/**
 * EventDispatcher constructor
 * @param {Object} tree       The tree object.
 * @param {Object} definition The definition object.
 * @constructor
 */
class EventDispatcher {
  constructor (tree, definition) {
    if (!tree || !_.isObject(tree) || !_.isFunction(tree.getViewBuilders)) {
      const err = new Error('Please pass a valid tree!');
      debug(err);
      throw err;
    }

    if (!definition || !_.isObject(definition)) {
      const err = new Error('Please pass a valid command definition!');
      debug(err);
      throw err;
    }

    this.tree = tree;

    this.definition = {
      correlationId: 'correlationId', // optional
      id: 'id', // optional
      name: 'name', // optional
      //      aggregateId: 'aggregate.id',    // optional
      //      context: 'context.name',        // optional
      //      aggregate: 'aggregate.name',    // optional
      payload: 'payload', // optional
      //      revision: 'revision'            // optional
      //      version: 'version',             // optional
      //      meta: 'meta'
      // optional, if defined theses values will be copied to the notification (can be used to transport information like userId, etc..)
    };

    this.definition = _.defaults(definition, this.definition);
  }

  /**
   * Returns the target information of this event.
   * @param {Object} evt The passed event.
   * @returns {{name: 'eventName', aggregateId: 'aggregateId', version: 0, aggregate: 'aggregateName', context: 'contextName'}}
   */
  getTargetInformation (evt) {
    if (!evt || !_.isObject(evt)) {
      const err = new Error('Please pass a valid event!');
      debug(err);
      throw err;
    }

    const name = dotty.get(evt, this.definition.name) || '';

    let version = 0;
    if (dotty.exists(evt, this.definition.version)) {
      version = dotty.get(evt, this.definition.version);
    }
    else {
      debug('no version found, handling as version: 0');
    }

    let aggregate = null;
    if (dotty.exists(evt, this.definition.aggregate)) {
      aggregate = dotty.get(evt, this.definition.aggregate);
    }
    else {
      debug('no aggregate found');
    }

    let context = null;
    if (dotty.exists(evt, this.definition.context)) {
      context = dotty.get(evt, this.definition.context);
    }
    else {
      debug('no context found');
    }

    return {
      name: name,
      version: version,
      aggregate: aggregate,
      context: context,
    };
  }

  /**
   * Dispatches an event.
   * @param {Object}   evt      The passed event.
   * @param {Function} callback The function, that will be called when this action is completed.
   *                            `function(errs, notifications){}`
   */
  dispatch (evt, callback) {
    if (!evt || !_.isObject(evt)) {
      const err = new Error('Please pass a valid event!');
      debug(err);
      throw err;
    }

    if (!callback || !_.isFunction(callback)) {
      const err = new Error('Please pass a valid callback!');
      debug(err);
      throw err;
    }

    const target = this.getTargetInformation(evt);

    const viewBuilders = this.tree.getViewBuilders(target);

    let errs = [];
    let notifications = [];

    const foundPrioSet = _.find(viewBuilders, function (vb) {
      return vb.priority < Infinity;
    });

    let eachMethod = 'each';
    if (foundPrioSet) {
      eachMethod = 'eachSeries';
    }

    async[eachMethod].call(async, viewBuilders, function (viewBuilder, callback) {
      viewBuilder.denormalize(evt, function (err, notis) {
        if (err) {
          debug(err);
          if (!errs.push) {
            const warn = new Error('ATTENTION! Already called back!');
            debug(warn);
            console.log(warn.stack);
            return;
          }
          errs.push(err);
        }

        if (notis && notis.length > 0) {
          notifications = notifications.concat(notis);
        }
        callback(null);
      });
    }, function () {
      if (errs.length === 0) {
        errs = null;
      }
      callback(errs, _.filter(notifications, function (n) {
        return !!n;
      }));
    });
  }
}


module.exports = EventDispatcher;
