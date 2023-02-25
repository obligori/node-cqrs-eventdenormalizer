'use strict';

const debug = require('debug')('denormalizer:orderQueue');

const _ = require('lodash');

const AlreadyDenormalizingError = require('#/errors/alreadyDenormalizingError');


/**
 * Queue constructor
 * @param {Object} options The options object. Like: { queueTimeout: 3000 }
 * @constructor
 */
class Queue {
  constructor (options) {
    this.queue = {};
    this.retries = {};
    this.options = options || {queueTimeout: 3000};
  }

  /**
   * Pushes a new item in the queue.
   * @param {String}   id     The aggregate id.
   * @param {String}   objId  The event id.
   * @param {Object}   object The event.
   * @param {Function} clb    The callback function for the event handle.
   * @param {Function} fn     The timeout function handle.
   */
  push (id, objId, object, clb, fn) {
    this.queue[id] = this.queue[id] || [];

    const alreadyInQueue = _.find(this.queue[id], function (o) {
      return o.id === objId;
    });

    if (alreadyInQueue) {
      debug('event already denormalizing [concatenatedId]=' + id + ', [evtId]=' + objId);
      clb(new AlreadyDenormalizingError('Event: [id]=' + objId + ', [evtId]=' + objId + ' already denormalizing!'), function (done) {
        done(null);
      });
      return;
    }

    this.queue[id].push({id: objId, payload: object, callback: clb});

    this.retries[id] = this.retries[id] || {};

    this.retries[id][objId] = this.retries[id][objId] || 0;

    if (fn) {
      const self = this;
      (function wait () {
        debug('wait called [concatenatedId]=' + id + ', [evtId]=' + objId);
        setTimeout(function () {
          const found = _.find(self.queue[id], function (o) {
            return o.id === objId;
          });
          if (found) {
            const loopCount = self.retries[id][objId]++;
            fn(loopCount, wait);
          }
        }, self.options.queueTimeout);
      })();
    }
  }

  /**
   * Returns the pending events for an aggregate.
   * @param {String} id The aggregate id.
   * @returns {Array}
   */
  get (id) {
    if (!this.queue[id] || this.queue[id].length === 0) {
      return null;
    }
    return this.queue[id];
  }

  /**
   * Removes an event from the queue.
   * @param {String} id    The aggregate id.
   * @param {String} objId The event id.
   */
  remove (id, objId) {
    if (this.queue[id]) {
      _.remove(this.queue[id], function (o) {
        return o.id === objId;
      });
    }

    if (objId && this.retries[id] && this.retries[id][objId]) {
      this.retries[id][objId] = 0;
    }
  }

  /**
   * NEVER USE THIS FUNCTION!!! ONLY FOR TESTS!
   * clears the complete queue...
   */
  clear () {
    this.queue = {};
    this.retries = {};
  }
}


module.exports = Queue;
