'use strict';

// Grab the util module that's bundled with Node
const util = require('util');

// Create a new custom Error constructor
function AlreadyDenormalizedError (msg) {
  // Pass the constructor to V8's
  // captureStackTrace to clean up the output
  Error.captureStackTrace(this, AlreadyDenormalizedError);

  // If defined, store a custom error message
  if (msg) {
    this.message = msg;
  }
}

// Extend our custom Error from Error
util.inherits(AlreadyDenormalizedError, Error);

// Give our custom error a name property. Helpful for logging the error later.
AlreadyDenormalizedError.prototype.name = AlreadyDenormalizedError.name;

module.exports = AlreadyDenormalizedError;
