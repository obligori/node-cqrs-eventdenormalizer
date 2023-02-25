'use strict';

const debug = require('debug')('denormalizer:structureLoader');

const _ = require('lodash');
const path = require('path');

const structureParser = require('lib/structure/structureParser');
const ViewBuilder = require('lib/definitions/viewBuilder');
const EventExtender = require('lib/definitions/eventExtender');
const PreEventExtender = require('lib/definitions/preEventExtender');
const Collection = require('lib/definitions/collection');


function isViewBuilder (item) {
  if (item.fileType !== 'js') {
    return false;
  }

  return item.value instanceof ViewBuilder;
}

function isEventExtender (item) {
  if (item.fileType !== 'js') {
    return false;
  }

  return item.value instanceof EventExtender && !(item.value instanceof PreEventExtender);
}

function isPreEventExtender (item) {
  if (item.fileType !== 'js') {
    return false;
  }

  return item.value instanceof PreEventExtender;
}

function isCollection (item) {
  if (item.fileType !== 'js') {
    return false;
  }

  return item.value instanceof Collection;
}

function defineName (item, invert) {
  let name = item.value.name;

  if (name === '') {
    item.name = name;
    return;
  }

  function defineNameByDir () {
    if (!name) {
      const splits = item.dottiedBase.split('.');
      name = splits[splits.length - 1];
    }

    if (!name) {
      const tmp = item.path.substring(0, item.path.lastIndexOf(path.sep + item.fileName));
      name = tmp.substring(tmp.lastIndexOf(path.sep) + 1);
    }
  }

  function defineNameByFileName () {
    if (!name) {
      name = item.fileName.substring(0, item.fileName.lastIndexOf('.'));
    }
  }

  if (invert) {
    defineNameByDir();
    defineNameByFileName();
  }
  else {
    defineNameByFileName();
    defineNameByDir();
  }

  item.name = name;
}

function scan (items) {
  const res = {
    viewBuilders: [],
    eventExtenders: [],
    preEventExtenders: [],
    collections: [],
  };

  items.forEach(function (item) {
    if (isViewBuilder(item)) {
      debug('found viewBuilder at: ' + item.path);
      defineName(item);
      item.value.name = item.name;
      res.viewBuilders.push(item);
      return;
    }

    if (isEventExtender(item)) {
      debug('found eventExtender at: ' + item.path);
      defineName(item);
      item.value.name = item.name;
      res.eventExtenders.push(item);
      return;
    }

    if (isPreEventExtender(item)) {
      debug('found preEventExtender at: ' + item.path);
      defineName(item);
      item.value.name = item.name;
      res.preEventExtenders.push(item);
      return;
    }

    if (isCollection(item)) {
      debug('found collection at: ' + item.path);
      defineName(item, true);
      item.value.name = item.name;
      res.collections.push(item);
      return;
    }
  });

  return res;
}

function analyze (dir, callback) {
  structureParser(dir, function (items) {
    return _.filter(items, function (i) {
      return isViewBuilder(i) || isEventExtender(i) || isPreEventExtender(i) || isCollection(i);
    });
  }, function (err, items, warns) {
    if (err) {
      return callback(err);
    }

    const res = scan(items);

    callback(null, res, warns);
  });
}

function reorderViewBuilders (obj) {
  obj.viewBuilders.forEach(function (objItem) {
    const foundCol = _.find(obj.collections, function (col) {
      if (objItem.dottiedBase.indexOf('.') >= 0) {
        return objItem.dottiedBase === col.dottiedBase || objItem.dottiedBase.indexOf(col.dottiedBase + '.') === 0;
      }
      else {
        return objItem.dottiedBase === col.dottiedBase;
      }
    });

    if (!foundCol) {
      return;
    }

    foundCol.value.addViewBuilder(objItem.value);
  });
}

function reorderEventExtenders (obj, ordered) {
  obj.eventExtenders.forEach(function (objItem) {
    const foundCol = _.find(obj.collections, function (col) {
      if (objItem.dottiedBase.indexOf('.') >= 0) {
        return objItem.dottiedBase === col.dottiedBase || objItem.dottiedBase.indexOf(col.dottiedBase + '.') === 0;
      }
      else {
        return objItem.dottiedBase === col.dottiedBase;
      }
    });

    if (!foundCol) {
      ordered.generalEventExtenders.push(objItem.value);
      return;
    }

    foundCol.value.addEventExtender(objItem.value);
  });
}

function reorderPreEventExtenders (obj, ordered) {
  obj.preEventExtenders.forEach(function (objItem) {
    const foundCol = _.find(obj.collections, function (col) {
      if (objItem.dottiedBase.indexOf('.') >= 0) {
        return objItem.dottiedBase === col.dottiedBase || objItem.dottiedBase.indexOf(col.dottiedBase + '.') === 0;
      }
      else {
        return objItem.dottiedBase === col.dottiedBase;
      }
    });

    if (!foundCol) {
      ordered.generalPreEventExtenders.push(objItem.value);
      return;
    }

    foundCol.value.addPreEventExtender(objItem.value);
  });
}

function reorder (obj) {
  const ordered = {
    collections: _.map(obj.collections, function (c) {
      return c.value;
    }),
    generalEventExtenders: [],
    generalPreEventExtenders: [],
  };

  reorderViewBuilders(obj);

  reorderEventExtenders(obj, ordered);

  reorderPreEventExtenders(obj, ordered);

  return ordered;
}

function load (dir, callback) {
  analyze(dir, function (err, dividedByTypes, warns) {
    if (err) {
      return callback(err);
    }

    const structured = reorder(dividedByTypes);

    callback(err, structured, warns);
  });
}

module.exports = load;
