'use strict';

const EventEmitter = require('events').EventEmitter;
const prequire = require('parent-require');
const uuid = require('uuid').v4;


/**
 * Guard constructor
 * @param {Object} options The options can have information like host, port, etc. [optional]
 */
class Guard extends EventEmitter {
  /**
   * Initiate communication with the lock.
   * @param  {Function} callback The function, that will be called when this action is completed. [optional]
   *                             `function(err, queue){}`
   */
  connect (callback) {
    implementError(callback);
  }

  /**
   * Terminate communication with the lock.
   * @param  {Function} callback The function, that will be called when this action is completed. [optional]
   *                             `function(err){}`
   */
  disconnect (callback) {
    implementError(callback);
  }

  /**
   * Use this function to obtain a new id.
   * @param  {Function} callback The function, that will be called when this action is completed.
   *                             `function(err, id){}` id is of type String.
   */
  getNewId (callback) {
    const id = uuid().toString();
    if (callback) callback(null, id);
  }

  /**
   * Use this function to obtain the revision by id.
   * @param {String}   id       The aggregate id.
   * @param {Function} callback The function, that will be called when this action is completed.
   *                             `function(err, revision){}` id is of type String.
   */
  get (id, callback) {
    implementError(callback);
  }

  /**
   * Updates the revision number.
   * @param {String}   id          The aggregate id.
   * @param {Number}   revision    The new revision number.
   * @param {Number}   oldRevision The old revision number.
   * @param {Function} callback    The function, that will be called when this action is completed.
   *                               `function(err, revision){}` revision is of type Number.
   */
  set (id, revision, oldRevision, callback) {
    implementError(callback);
  }

  /**
   * Saves the last event.
   * @param {Object}   evt      The event that should be saved.
   * @param {Function} callback The function, that will be called when this action is completed.
   *                            `function(err){}`
   */
  saveLastEvent (evt, callback) {
    implementError(callback);
  }

  /**
   * Gets the last event.
   * @param {Function} callback The function, that will be called when this action is completed.
   *                            `function(err, evt){}` evt is of type Object.
   */
  getLastEvent (callback) {
    implementError(callback);
  }

  /**
   * NEVER USE THIS FUNCTION!!! ONLY FOR TESTS!
   * clears the complete store...
   * @param {Function} callback the function that will be called when this action has finished [optional]
   */
  clear (callback) {
    implementError(callback);
  }
}


function implementError (callback) {
  const err = new Error('Please implement this function!');
  if (callback) callback(err);
  throw err;
}


Guard.use = function (toRequire) {
  let required;
  try {
    required = require(toRequire);
  }
  catch (e) {
    // workaround when `npm link`'ed for development
    required = prequire(toRequire);
  }
  return required;
};

module.exports = Guard;
