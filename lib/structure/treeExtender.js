'use strict';

const debug = require('debug')('denormalizer:treeExtender');

const _ = require('lodash');

class TreeExtender {
  constructor (tree) {
    if (!tree || _.isEmpty(tree)) {
      debug('no tree injected');
    }
    this.tree = tree;
  }

  getInfo () {
    if (!this.tree || _.isEmpty(this.tree)) {
      debug('no tree injected');
      return null;
    }

    const info = {
      collections: [],
      generalEventExtenders: [],
      generalPreEventExtenders: [],
    };

    this.tree.collections.forEach(function (col) {
      const c = {name: col.name, viewBuilders: [], eventExtenders: [], preEventExtenders: []};

      col.viewBuilders.forEach(function (vB) {
        c.viewBuilders.push({
          name: vB.name,
          aggregate: vB.aggregate,
          context: vB.context,
          version: vB.version,
          priority: vB.priority,
        });
      });

      col.eventExtenders.forEach(function (evtExt) {
        c.eventExtenders.push({
          name: evtExt.name,
          aggregate: evtExt.aggregate,
          context: evtExt.context,
          version: evtExt.version,
        });
      });

      col.preEventExtenders.forEach(function (evtExt) {
        c.preEventExtenders.push({
          name: evtExt.name,
          aggregate: evtExt.aggregate,
          context: evtExt.context,
          version: evtExt.version,
        });
      });

      info.collections.push(c);
    });

    this.tree.generalEventExtenders.forEach(function (evtExt) {
      info.generalEventExtenders.push({
        name: evtExt.name,
        aggregate: evtExt.aggregate,
        context: evtExt.context,
        version: evtExt.version,
      });
    });

    this.tree.generalPreEventExtenders.forEach(function (evtExt) {
      info.generalPreEventExtenders.push({
        name: evtExt.name,
        aggregate: evtExt.aggregate,
        context: evtExt.context,
        version: evtExt.version,
      });
    });

    return info;
  }

  getViewBuilders (query) {
    if (!this.tree || _.isEmpty(this.tree)) {
      debug('no tree injected');
      return null;
    }

    let res = [];

    this.tree.collections.forEach(function (col) {
      const vBs = col.getViewBuilders(query);
      res = res.concat(vBs);
    });

    res = _.sortBy(res, function (vb) {
      return vb.priority;
    });

    return res;
  }

  getCollections () {
    if (!this.tree || _.isEmpty(this.tree)) {
      debug('no tree injected');
      return null;
    }

    return this.tree.collections;
  }

  getEventExtender (query) {
    if (!this.tree || _.isEmpty(this.tree)) {
      debug('no tree injected');
      return null;
    }

    let evtExt;

    for (let i = 0, len = this.tree.collections.length; i < len; i++) {
      const col = this.tree.collections[i];
      evtExt = col.getEventExtender(query);
      if (evtExt) {
        return evtExt;
      }
    }

    for (let j = 0, lenJ = this.tree.generalEventExtenders.length; j < lenJ; j++) {
      evtExt = this.tree.generalEventExtenders[j];
      if (evtExt &&
            evtExt.name === query.name &&
            (evtExt.version === query.version || evtExt.version === -1) &&
            (evtExt.aggregate === query.aggregate || !query.aggregate || ! evtExt.aggregate) &&
            (evtExt.context === query.context || !query.context || ! evtExt.context)) {
        return evtExt;
      }
    }

    for (let k = 0, lenK = this.tree.generalEventExtenders.length; k < lenK; k++) {
      evtExt = this.tree.generalEventExtenders[k];
      if (evtExt &&
            evtExt.name === '' &&
            (evtExt.version === query.version || evtExt.version === -1) &&
            (evtExt.aggregate === query.aggregate || !query.aggregate || ! evtExt.aggregate) &&
            (evtExt.context === query.context)) {
        return evtExt;
      }
    }

    for (let l = 0, lenL = this.tree.generalEventExtenders.length; l < lenL; l++) {
      evtExt = this.tree.generalEventExtenders[l];
      if (evtExt &&
          evtExt.name === '' &&
          (evtExt.version === query.version || evtExt.version === -1) &&
          (evtExt.aggregate === query.aggregate || !query.aggregate || ! evtExt.aggregate) &&
          (evtExt.context === query.context || !query.context || ! evtExt.context)) {
        return evtExt;
      }
    }

    return null;
  }

  getPreEventExtender (query) {
    if (!this.tree || _.isEmpty(this.tree)) {
      debug('no tree injected');
      return null;
    }

    let evtExt;

    for (let i = 0, len = this.tree.collections.length; i < len; i++) {
      const col = this.tree.collections[i];
      evtExt = col.getPreEventExtender(query);
      if (evtExt) {
        return evtExt;
      }
    }

    for (let j = 0, lenJ = this.tree.generalPreEventExtenders.length; j < lenJ; j++) {
      evtExt = this.tree.generalPreEventExtenders[j];
      if (evtExt &&
            evtExt.name === query.name &&
            (evtExt.version === query.version || evtExt.version === -1) &&
            (evtExt.aggregate === query.aggregate || !query.aggregate || ! evtExt.aggregate) &&
            (evtExt.context === query.context || !query.context || ! evtExt.context)) {
        return evtExt;
      }
    }

    for (let k = 0, lenK = this.tree.generalPreEventExtenders.length; k < lenK; k++) {
      evtExt = this.tree.generalPreEventExtenders[k];
      if (evtExt &&
            evtExt.name === '' &&
            (evtExt.version === query.version || evtExt.version === -1) &&
            (evtExt.aggregate === query.aggregate || !query.aggregate || ! evtExt.aggregate) &&
            (evtExt.context === query.context)) {
        return evtExt;
      }
    }

    for (let l = 0, lenL = this.tree.generalPreEventExtenders.length; l < lenL; l++) {
      evtExt = this.tree.generalPreEventExtenders[l];
      if (evtExt &&
          evtExt.name === '' &&
          (evtExt.version === query.version || evtExt.version === -1) &&
          (evtExt.aggregate === query.aggregate || !query.aggregate || ! evtExt.aggregate) &&
          (evtExt.context === query.context || !query.context || ! evtExt.context)) {
        return evtExt;
      }
    }

    return null;
  }

  defineOptions (options) {
    if (!this.tree || _.isEmpty(this.tree)) {
      debug('no tree injected');
      return this;
    }

    this.tree.collections.forEach(function (col) {
      col.defineOptions(options);

      col.getViewBuilders().forEach(function (vB) {
        vB.defineOptions(options);
      });

      col.getEventExtenders().forEach(function (eExt) {
        eExt.defineOptions(options);
      });

      col.getPreEventExtenders().forEach(function (eExt) {
        eExt.defineOptions(options);
      });
    });

    this.tree.generalEventExtenders.forEach(function (eExt) {
      eExt.defineOptions(options);
    });

    this.tree.generalPreEventExtenders.forEach(function (eExt) {
      eExt.defineOptions(options);
    });

    return this;
  }

  defineNotification (definition) {
    if (!this.tree || _.isEmpty(this.tree)) {
      debug('no tree injected');
      return this;
    }

    this.tree.collections.forEach(function (col) {
      col.defineNotification(definition);

      col.getViewBuilders().forEach(function (vB) {
        vB.defineNotification(definition);
      });

      col.getEventExtenders().forEach(function (eExt) {
        eExt.defineNotification(definition);
      });

      col.getPreEventExtenders().forEach(function (eExt) {
        eExt.defineNotification(definition);
      });
    });

    this.tree.generalEventExtenders.forEach(function (eExt) {
      eExt.defineNotification(definition);
    });

    this.tree.generalPreEventExtenders.forEach(function (eExt) {
      eExt.defineNotification(definition);
    });

    return this;
  }

  defineEvent (definition) {
    if (!this.tree || _.isEmpty(this.tree)) {
      debug('no tree injected');
      return this;
    }

    this.tree.collections.forEach(function (col) {
      col.defineEvent(definition);

      col.getViewBuilders().forEach(function (vB) {
        vB.defineEvent(definition);
      });

      col.getEventExtenders().forEach(function (eExt) {
        eExt.defineEvent(definition);
      });

      col.getPreEventExtenders().forEach(function (eExt) {
        eExt.defineEvent(definition);
      });
    });

    this.tree.generalEventExtenders.forEach(function (eExt) {
      eExt.defineEvent(definition);
    });

    this.tree.generalPreEventExtenders.forEach(function (eExt) {
      eExt.defineEvent(definition);
    });
    return this;
  }

  useRepository (repository) {
    if (!this.tree || _.isEmpty(this.tree)) {
      debug('no tree injected');
      return this;
    }

    this.tree.collections.forEach(function (col) {
      col.useRepository(repository);
    });
    return this;
  }

  idGenerator (getNewId) {
    if (!getNewId || !_.isFunction(getNewId)) {
      const err = new Error('Please pass a valid function!');
      debug(err);
      throw err;
    }

    if (!this.tree || _.isEmpty(this.tree)) {
      debug('no tree injected');
      return this;
    }

    this.tree.collections.forEach(function (col) {
      col.getViewBuilders().forEach(function (vB) {
        vB.idGenerator(getNewId);
      });
    });
    return this;
  }
}


module.exports = TreeExtender;
