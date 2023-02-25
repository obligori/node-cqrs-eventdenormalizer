'use strict';


const EventExtender = require('denormalizer/lib/definitions/eventExtender');

/**
 * PreEventExtender constructor
 * @param {Object}             meta     Meta infos like: { name: 'name', version: 1, payload: 'some.path' }
 * @param {Function || String} evtExtFn Function handle
 *                                      `function(evt, col, callback){}`
 * @constructor
 */
class PreEventExtender extends EventExtender {}


module.exports = PreEventExtender;
