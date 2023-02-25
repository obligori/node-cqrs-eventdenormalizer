'use strict';

const definitions = {
  ViewBuilder: require('denormalizer/lib/definitions/viewBuilder'),
  EventExtender: require('denormalizer/lib/definitions/eventExtender'),
  PreEventExtender: require('denormalizer/lib/definitions/preEventExtender'),
  Collection: require('denormalizer/lib/definitions/collection'),
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
