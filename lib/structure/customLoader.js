'use strict';

const definitions = {
  ViewBuilder: require('lib/definitions/viewBuilder'),
  EventExtender: require('lib/definitions/eventExtender'),
  PreEventExtender: require('lib/definitions/preEventExtender'),
  Collection: require('lib/definitions/collection'),
};

module.exports = function (loader) {
  return function (denormalizerPath, callback) {
    const options = {
      denormalizerPath: denormalizerPath,
      definitions: definitions,
    };

    let tree;
    try {
      const loadedTree = loader(options);
      tree = {
        generalPreEventExtenders: loadedTree.preEventExtenders || [],
        collections: loadedTree.collections,
        generalEventExtenders: loadedTree.eventExtenders || [],
      };
    }
    catch (e) {
      return callback(e);
    }

    return callback(null, tree);
  };
};
