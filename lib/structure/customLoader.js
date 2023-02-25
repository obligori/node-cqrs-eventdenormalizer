'use strict';

const definitions = {
  ViewBuilder: require('#/definitions/viewBuilder'),
  EventExtender: require('#/definitions/eventExtender'),
  PreEventExtender: require('#/definitions/preEventExtender'),
  Collection: require('#/definitions/collection'),
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
