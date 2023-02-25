'use strict';

const debug = require('debug')('denormalizer:replayHandler');

const _ = require('lodash');
const async = require('async');
const dotty = require('dotty');


/**
 * ReplayHandler constructor
 * @param {Object} disp    The dispatcher object.
 * @param {Object} store   The store object.
 * @param {Object} def     The definition object.
 * @param {Object} options The options object.
 * @constructor
 */
class ReplayHandler {
  constructor (disp, store, def, options) {
    this.dispatcher = disp;

    this.store = store;

    def = def || {};

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

    this.definition = _.defaults(def, this.definition);

    options = options || {};

    this.options = options;
  }

  /**
   * Clears all collections and the revisionGuardStore.
   * @param {Function} callback The function, that will be called when this action is completed.
   *                            `function(err){}`
   */
  clear (callback) {
    const self = this;
    async.parallel([
      function (callback) {
        async.each(self.dispatcher.tree.getCollections(), function (col, callback) {
          if (col.noReplay) {
            return callback(null);
          }
          col.repository.clear(callback);
        }, callback);
      },
      function (callback) {
        self.store.clear(callback);
      },
    ], callback);
  }

  /**
   * Returns the concatenated id (more unique)
   * @param {Object}   evt The passed eventt.
   * @returns {string}
   */
  getConcatenatedId (evt) {
    let aggregateId = '';
    if (dotty.exists(evt, this.definition.aggregateId)) {
      aggregateId = dotty.get(evt, this.definition.aggregateId);
    }

    let aggregate = '';
    if (dotty.exists(evt, this.definition.aggregate)) {
      aggregate = dotty.get(evt, this.definition.aggregate);
    }

    let context = '';
    if (dotty.exists(evt, this.definition.context)) {
      context = dotty.get(evt, this.definition.context);
    }

    return context + aggregate + aggregateId;
  }

  /**
   * Updates the revision in the store.
   * @param {Object}   revisionMap The revision map.
   * @param {Function} callback    The function, that will be called when this action is completed.
   *                               `function(err){}`
   */
  updateRevision (revisionMap, callback) {
    const self = this;
    const ids = _.keys(revisionMap);
    async.each(ids, function (id, callback) {
      self.store.get(id, function (err, rev) {
        if (err) {
          return callback(err);
        }
        self.store.set(id, revisionMap[id] + 1, rev, callback);
      });
    }, callback);
  }

  /**
   * Replays all passed events.
   * @param {Array}    evts     The passed array of events.
   * @param {Function} callback The function, that will be called when this action is completed.
   *                            `function(err){}`
   */
  replay (evts, callback) {
    this.replayStreamed(function (replay, done) {
      evts.forEach(function (evt) {
        replay(evt);
      });
      done(callback);
    });
  }

  /**
   * Replays in a streamed way.
   * @param {Function} fn The function that will be called with the replay function and the done function.
   *                      `function(replay, done){}`
   */
  replayStreamed (fn) {
    const self = this;

    let evtCount = 0;
    const eventQueue = [];
    let eventQueueHandling = false;

    const errs = [];
    let doneCalled = false;
    let doneClb = null;

    const revisionMap = {};
    const collections = {};

    let lastEvent;

    let seenEvents = {};

    const replay = function (evt) {
      const evtId = dotty.get(evt, self.definition.id);
      lastEvent = evt;
      let concatenatedId;
      if (!!self.definition.revision && dotty.exists(evt, self.definition.revision) &&
        !!self.definition.aggregateId && dotty.exists(evt, self.definition.aggregateId)) {
        concatenatedId = self.getConcatenatedId(evt);
        revisionMap[concatenatedId] = dotty.get(evt, self.definition.revision);
      }

      let concatenatedWithEventId = evtId;
      if (concatenatedId) concatenatedWithEventId = concatenatedId + ':' + evtId;
      if (seenEvents[concatenatedWithEventId]) return;
      seenEvents[concatenatedWithEventId] = true;

      const target = self.dispatcher.getTargetInformation(evt);

      const viewBuilders = []; let foundPrioSet = false;

      _.each(self.dispatcher.tree.getViewBuilders(target), function (vb) {
        if (!vb.collection.noReplay) {
          viewBuilders.push(vb);

          if (!foundPrioSet && vb.priority < Infinity) {
            foundPrioSet = true;
          }

          if (!collections[vb.collection.workerId]) {
            vb.collection.isReplaying = true;
            collections[vb.collection.workerId] = vb.collection;
          }
        }
      });

      if (foundPrioSet) {
        _.each(viewBuilders, function (vb) {
          eventQueue.push({event: evt, viewBuilders: [vb]});
          evtCount++;
        });
      }
      else {
        eventQueue.push({event: evt, viewBuilders: viewBuilders});
        evtCount += viewBuilders.length;
      }

      function handleNext () {
        if (evtCount <= 0 && doneCalled) {
          doneLater();
          return;
        }

        if (eventQueue.length > 0) {
          const task = eventQueue.shift();
          let e = task.event; const vbs = task.viewBuilders;

          async.series([
            function (clb) {
              const preEventExtender = self.dispatcher.tree.getPreEventExtender(self.dispatcher.getTargetInformation(e));
              if (!preEventExtender) return clb(null);
              preEventExtender.extend(e, function (err, extEvt) {
                if (err) return clb(err);
                e = extEvt || e;
                clb(null);
              });
            },
            function (clb) {
              async.each(vbs, function (vb, callback) {
                vb.denormalize(e, function (err) {
                  --evtCount;
                  if (err) {
                    debug(err);
                    errs.push(err);
                  }

                  callback();
                });
              }, clb);
            },
          ], function () {
            handleNext();
          });
        }
        else if (eventQueueHandling) {
          eventQueueHandling = false;
        }
      }

      if (!eventQueueHandling) {
        eventQueueHandling = true;
        process.nextTick(handleNext);
      }
    };

    function doneLater () {
      if (doneCalled) {
        done(doneClb);
      }
    }

    var done = function (callback) {
      if (evtCount > 0) {
        doneCalled = true;
        doneClb = callback;
        return;
      }

      async.parallel([
        function (callback) {
          async.each(_.values(collections), function (col, callback) {
            col.saveReplayingVms(callback);
          }, callback);
        },
        function (callback) {
          self.updateRevision(revisionMap, callback);
        },
        function (callback) {
          self.store.saveLastEvent(lastEvent, callback);
        },
      ], function (err) {
        seenEvents = {};

        if (err) {
          debug(err);
          errs.push(err);
        }

        if (errs.length === 0) {
          if (callback) {
            callback(null);
          }
          return;
        }

        if (callback) {
          callback(errs);
        }
      });
    };

    fn(replay, done);
  }
}

module.exports = ReplayHandler;
