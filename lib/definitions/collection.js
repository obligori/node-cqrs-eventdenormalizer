'use strict';

require('module-alias/register');

const debug = require('debug')('denormalizer:collection');

const _ = require('lodash');
const uuid = require('uuid').v4;
const async = require('async');
const viewmodel = require('viewmodel');
const sift = require('sift');
const flatten = require('flat');

const Definition = require('denormalizer/lib/definitionBase');


/**
 * Collection constructor
 * @param {Object} meta            Meta infos like: { name: 'name' }
 * @param {Object} modelInitValues Initialization values for model like: { emails: [] } [optional]
 * @constructor
 */
class Collection extends Definition {
  constructor (meta, modelInitValues) {
    super(meta);
    // used for replay...
    this.workerId = uuid().toString();
    this.isReplaying = false;
    this.replayingVms = {};
    this.replayingVmsToDelete = {};
    meta = meta || {};
    this.repositorySettings = meta.repositorySettings || {};
    this.defaultPayload = meta.defaultPayload || '';
    this.indexes = meta.indexes || [];
    this.noReplay = !!meta.noReplay || false;
    this.modelInitValues = modelInitValues || {};
    this.viewBuilders = [];
    this.eventExtenders = [];
    this.preEventExtenders = [];
  }

  /**
   * Injects the needed repository.
   * @param {Object} repository The repository object to inject.
   */
  useRepository (repository) {
    if (!repository || !_.isObject(repository)) {
      const err = new Error('Please pass a valid repository!');
      debug(err);
      throw err;
    }

    const extendObject = {
      collectionName: this.name,
      indexes: this.indexes,
    };

    if (repository.repositoryType && this.repositorySettings[repository.repositoryType]) {
      extendObject.repositorySettings = {};
      extendObject.repositorySettings[repository.repositoryType] = this.repositorySettings[repository.repositoryType];
    }

    this.repository = repository.extend(extendObject);
  }

  /**
   * Add viewBuilder module.
   * @param {ViewBuilder} viewBuilder The viewBuilder module to be injected.
   */
  addViewBuilder (viewBuilder) {
    if (!viewBuilder || !_.isObject(viewBuilder)) {
      const err = new Error('Please inject a valid view builder object!');
      debug(err);
      throw err;
    }
    if (viewBuilder.payload === null || viewBuilder.payload === undefined) {
      viewBuilder.payload = this.defaultPayload;
    }
    if (this.viewBuilders.indexOf(viewBuilder) < 0) {
      viewBuilder.useCollection(this);
      this.viewBuilders.push(viewBuilder);
    }
  }

  /**
   * Add eventExtender module.
   * @param {EventExtender} eventExtender The eventExtender module to be injected.
   */
  addEventExtender (eventExtender) {
    if (!eventExtender || !_.isObject(eventExtender)) {
      const err = new Error('Please inject a valid event extender object!');
      debug(err);
      throw err;
    }
    if (this.eventExtenders.indexOf(eventExtender) < 0) {
      eventExtender.useCollection(this);
      this.eventExtenders.push(eventExtender);
    }
  }

  /**
   * Add preEventExtender module.
   * @param {PreEventExtender} preEventExtender The preEventExtender module to be injected.
   */
  addPreEventExtender (preEventExtender) {
    if (!preEventExtender || !_.isObject(preEventExtender)) {
      const err = new Error('Please inject a valid event extender object!');
      debug(err);
      throw err;
    }
    if (this.preEventExtenders.indexOf(preEventExtender) < 0) {
      preEventExtender.useCollection(this);
      this.preEventExtenders.push(preEventExtender);
    }
  }

  /**
   * Returns the viewBuilder module by query.
   * @param {Object} query The query object. [optional] If not passed, all viewBuilders will be returned.
   * @returns {Array}
   */
  getViewBuilders (query) {
    if (!query || !_.isObject(query)) {
      return this.viewBuilders;
    }
    query.name = query.name || '';
    query.version = query.version || 0;
    query.aggregate = query.aggregate || null;
    query.context = query.context || null;
    let found = _.filter(this.viewBuilders, function (vB) {
      return vB.name === query.name &&
            (vB.version === query.version || vB.version === -1) &&
            (vB.aggregate === query.aggregate) &&
            (vB.context === query.context);
    });
    if (found.length !== 0) {
      return found;
    }
    found = _.filter(this.viewBuilders, function (vB) {
      return vB.name === query.name &&
        (vB.version === query.version || vB.version === -1) &&
        (vB.aggregate === query.aggregate) &&
        (vB.context === query.context || !query.context);
    });
    if (found.length !== 0) {
      return found;
    }
    found = _.filter(this.viewBuilders, function (vB) {
      return vB.name === query.name &&
        (vB.version === query.version || vB.version === -1) &&
        (vB.aggregate === query.aggregate || !query.aggregate) &&
        (vB.context === query.context || !query.context);
    });
    if (found.length !== 0) {
      return found;
    }
    return _.filter(this.viewBuilders, function (vB) {
      return vB.name === '' &&
        (vB.version === query.version || vB.version === -1) &&
        (vB.aggregate === query.aggregate || !query.aggregate) &&
        (vB.context === query.context || !query.context);
    });
  }

  /**
   * Returns the eventExtender module by query.
   * @param {Object} query The query object.
   * @returns {EventExtender}
   */
  getEventExtender (query) {
    if (!query || !_.isObject(query)) {
      const err = new Error('Please pass a valid query object!');
      debug(err);
      throw err;
    }
    query.name = query.name || '';
    query.version = query.version || 0;
    query.aggregate = query.aggregate || null;
    query.context = query.context || null;
    let found = _.find(this.eventExtenders, function (evExt) {
      return evExt.name === query.name &&
        (evExt.version === query.version || evExt.version === -1) &&
        (evExt.aggregate === query.aggregate) &&
        (evExt.context === query.context);
    });
    if (found) {
      return found;
    }
    found = _.find(this.eventExtenders, function (evExt) {
      return evExt.name === query.name &&
        (evExt.version === query.version || evExt.version === -1) &&
        (evExt.aggregate === query.aggregate || !query.aggregate || !evExt.aggregate) &&
        (evExt.context === query.context);
    });
    if (found) {
      return found;
    }
    found = _.find(this.eventExtenders, function (evExt) {
      return evExt.name === query.name &&
        (evExt.version === query.version || evExt.version === -1) &&
        (evExt.aggregate === query.aggregate || !query.aggregate || !evExt.aggregate) &&
        (evExt.context === query.context || !query.context || !evExt.context);
    });
    if (found) {
      return found;
    }
    return _.find(this.eventExtenders, function (evExt) {
      return evExt.name === '' &&
        (evExt.version === query.version || evExt.version === -1) &&
        (evExt.aggregate === query.aggregate || !query.aggregate || !evExt.aggregate) &&
        (evExt.context === query.context || !query.context || !evExt.context);
    });
  }

  /**
   * Returns the preEventExtender module by query.
   * @param {Object} query The query object.
   * @returns {PreEventExtender}
   */
  getPreEventExtender (query) {
    if (!query || !_.isObject(query)) {
      const err = new Error('Please pass a valid query object!');
      debug(err);
      throw err;
    }
    query.name = query.name || '';
    query.version = query.version || 0;
    query.aggregate = query.aggregate || null;
    query.context = query.context || null;
    let found = _.find(this.preEventExtenders, function (evExt) {
      return evExt.name === query.name &&
        (evExt.version === query.version || evExt.version === -1) &&
        (evExt.aggregate === query.aggregate) &&
        (evExt.context === query.context);
    });
    if (found) {
      return found;
    }
    found = _.find(this.preEventExtenders, function (evExt) {
      return evExt.name === query.name &&
        (evExt.version === query.version || evExt.version === -1) &&
        (evExt.aggregate === query.aggregate || !query.aggregate || !evExt.aggregate) &&
        (evExt.context === query.context);
    });
    if (found) {
      return found;
    }
    found = _.find(this.preEventExtenders, function (evExt) {
      return evExt.name === query.name &&
        (evExt.version === query.version || evExt.version === -1) &&
        (evExt.aggregate === query.aggregate || !query.aggregate || !evExt.aggregate) &&
        (evExt.context === query.context || !query.context || !evExt.context);
    });
    if (found) {
      return found;
    }
    return _.find(this.preEventExtenders, function (evExt) {
      return evExt.name === '' &&
        (evExt.version === query.version || evExt.version === -1) &&
        (evExt.aggregate === query.aggregate || !query.aggregate || !evExt.aggregate) &&
        (evExt.context === query.context || !query.context || !evExt.context);
    });
  }

  /**
   * Returns all eventExtender modules.
   * @returns {Array}
   */
  getEventExtenders () {
    return this.eventExtenders;
  }

  /**
   * Returns all preEventExtender modules.
   * @returns {Array}
   */
  getPreEventExtenders () {
    return this.preEventExtenders;
  }

  /**
   * Use this function to obtain a new id.
   * @param {Function} callback The function, that will be called when the this action is completed.
   *                            `function(err, id){}` id is of type String.
   */
  getNewId (callback) {
    this.repository.getNewId(function (err, newId) {
      if (err) {
        debug(err);
        return callback(err);
      }
      callback(null, newId);
    });
  }

  /**
   * Save the passed viewModel object in the read model.
   * @param {Object}   vm       The viewModel object.
   * @param {Function} callback The function, that will be called when the this action is completed. [optional]
   *                            `function(err){}`
   */
  saveViewModel (vm, callback) {
    if (this.isReplaying) {
      vm.actionOnCommitForReplay = vm.actionOnCommit;
      // Clone the values to be sure no reference mistakes happen!
      if (vm.attributes) {
        const flatAttr = flatten(vm.attributes);
        const undefines = [];
        _.each(flatAttr, function (v, k) {
          if (v === undefined) {
            undefines.push(k);
          }
        });
        vm.attributes = vm.toJSON();
        _.each(undefines, function (k) {
          vm.set(k, undefined);
        });
      }

      this.replayingVms[vm.id] = vm;
      if (vm.actionOnCommit === 'delete') {
        delete this.replayingVms[vm.id];
        if (!this.replayingVmsToDelete[vm.id]) this.replayingVmsToDelete[vm.id] = vm;
      }
      if (vm.actionOnCommit === 'create') {
        vm.actionOnCommit = 'update';
      }
      return callback(null);
    }
    this.repository.commit(vm, callback);
  }

  /**
   * Loads a viewModel object by id.
   * @param {String}   id       The viewModel id.
   * @param {Function} callback The function, that will be called when the this action is completed.
   *                            `function(err, vm){}` vm is of type Object
   */
  loadViewModel (id, callback) {
    if (this.isReplaying) {
      if (this.replayingVms[id]) {
        return callback(null, this.replayingVms[id]);
      }
      if (this.replayingVmsToDelete[id]) {
        const vm = new viewmodel.ViewModel({id: id}, this.repository);
        const clonedInitValues = _.cloneDeep(this.modelInitValues);
        for (const prop in clonedInitValues) {
          if (!vm.has(prop)) {
            vm.set(prop, clonedInitValues[prop]);
          }
        }
        this.replayingVms[vm.id] = vm;
        return callback(null, this.replayingVms[id]);
      }
    }
    const self = this;
    this.repository.get(id, function (err, vm) {
      if (err) {
        debug(err);
        return callback(err);
      }
      if (!vm) {
        err = new Error('No vm object returned!');
        debug(err);
        return callback(err);
      }
      const clonedInitValues = _.cloneDeep(self.modelInitValues);
      for (const prop in clonedInitValues) {
        if (!vm.has(prop)) {
          vm.set(prop, clonedInitValues[prop]);
        }
      }
      if (self.isReplaying) {
        if (!self.replayingVms[vm.id]) {
          self.replayingVms[vm.id] = vm;
        }
        return callback(null, self.replayingVms[vm.id]);
      }
      callback(null, vm);
    });
  }

  /**
   * Loads a viewModel object by id if exists.
   * @param {String}   id       The viewModel id.
   * @param {Function} callback The function, that will be called when the this action is completed.
   *                            `function(err, vm){}` vm is of type Object or null
   */
  loadViewModelIfExists (id, callback) {
    this.loadViewModel(id, function (err, vm) {
      if (err) {
        return callback(err);
      }

      if (!vm || vm.actionOnCommit === 'create') {
        return callback(null, null);
      }

      callback(null, vm);
    });
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
    const self = this;

    const localFoundVmsDict = {}; // Dictionary to have O(1) lookups
    let localFoundVms = [];
    if (this.isReplaying) {
      localFoundVms = _.reduce(this.replayingVms, function (result, vm) {
        // if (sift(query, [vm.toJSON()]).length > 0) { // We just read, so this is ok and faster!
        if (sift(query, [vm.attributes]).length > 0) {
          const newLen = result.push(vm);
          localFoundVmsDict[vm.id] = newLen - 1;
        }
        return result;
      }, []);
    }

    this.repository.find(query, queryOptions, function (err, serverVms) {
      if (err) {
        debug(err);
        return callback(err);
      }

      // We will now enhance the local replayingVms whith the results from the server
      // while keeping the local vms if it already exists.
      if (self.isReplaying) {
        // We preffer the local vms but add the server vm if no local available.
        const localAndServerVms = localFoundVms.concat(serverVms); // The order of this concat matters since we preffer the local ones.
        const resultDict = {}; // Dictionary to have O(1) lookups within the reduce loop.

        const uniqueLocalPrefferedVms = _.reduce(localAndServerVms, function (result, vm) {
          const isDeleted = !!self.replayingVmsToDelete[vm.id];
          const alreadyInResult = !!resultDict[vm.id];
          let localVm = null;
          let resultVm = null;
          if (isDeleted || alreadyInResult) return result;

          // No result found for query wihtin replayingVm since changes have been applied to the
          // replayingVm's already but from the server we retrieve a result.
          if (!localFoundVmsDict[vm.id] && self.replayingVms[vm.id]) {
            localVm = self.replayingVms[vm.id];
          }

          // Preffer local vm if available
          localVm = localVm || localFoundVms[localFoundVmsDict[vm.id]];
          if (localVm) {
            resultVm = localVm;
          }
          else {
            // Enhance server vm with initial values if not yet available
            const clonedInitValues = _.cloneDeep(self.modelInitValues);
            for (const prop in clonedInitValues) {
              if (!vm.has(prop)) {
                vm.set(prop, clonedInitValues[prop]);
              }
            }
            resultVm = vm;
          }

          result.push(resultVm);
          resultDict[resultVm.id] = true;
          if (!self.replayingVms[vm.id]) {
            self.replayingVms[vm.id] = resultVm;
          }

          return result;
        }, []);

        return callback(null, uniqueLocalPrefferedVms);
      }
      callback(null, serverVms);
    });
  }

  /**
   * Saves all replaying viewmodels.
   * @param {Function} callback The function, that will be called when the this action is completed.
   *                             `function(err){}`
   */
  saveReplayingVms (callback) {
    if (!this.isReplaying) {
      const err = new Error('Not in replay mode!');
      debug(err);
      return callback(err);
    }
    const replVms = _.values(this.replayingVms);
    const replVmsToDelete = _.values(this.replayingVmsToDelete);
    const self = this;

    function commit (vm, callback) {
      if (!vm.actionOnCommitForReplay) {
        return callback(null);
      }
      vm.actionOnCommit = vm.actionOnCommitForReplay;
      delete vm.actionOnCommitForReplay;
      self.repository.commit(vm, function (err) {
        if (err) {
          debug(err);
          debug(vm);
        }
        callback(err);
      });
    }

    function prepareVmsForBulkCommit (vms) {
      return _.map(_.filter(vms, function (vm) {
        return vm.actionOnCommitForReplay;
      }), function (vm) {
        vm.actionOnCommit = vm.actionOnCommitForReplay;
        delete vm.actionOnCommitForReplay;
        return vm;
      });
    }

    function bulkCommit (vms, callback) {
      if (vms.length === 0) return callback(null);
      self.repository.bulkCommit(prepareVmsForBulkCommit(vms), function (err) {
        if (err) {
          debug(err);
        }
        callback(err);
      });
    }

    async.series([
      function (callback) {
        if (self.repository.bulkCommit) {
          return bulkCommit(replVmsToDelete, callback);
        }
        async.each(replVmsToDelete, commit, callback);
      },
      function (callback) {
        if (self.repository.bulkCommit) {
          return bulkCommit(replVms, callback);
        }
        async.each(replVms, commit, callback);
      },
    ], function (err) {
      if (err) {
        debug(err);
      }
      self.replayingVms = {};
      self.replayingVmsToDelete = {};
      self.isReplaying = false;
      callback(err);
    });
  }
}

module.exports = Collection;
