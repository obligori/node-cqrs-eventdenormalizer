'use strict';

const debug = require('debug')('denormalizer:structureParser');

const _ = require('lodash');
const fs = require('fs');
const path = require('path');

const validFileTypes = ['js'];

function isValidFileType (fileName) {
  let index = fileName.lastIndexOf('.');
  if (index < 0) {
    return false;
  }
  const fileType = fileName.substring(index + 1);
  index = validFileTypes.indexOf(fileType);
  if (index < 0) {
    return false;
  }
  return validFileTypes[index];
}

function loadPaths (dir, callback) {
  dir = path.resolve(dir);

  let results = [];
  fs.readdir(dir, function (err, list) {
    if (err) {
      debug(err);
      return callback(err);
    }

    let pending = list.length;

    if (pending === 0) return callback(null, results);

    list.forEach(function (file) {
      const pathFull = path.join(dir, file);
      fs.stat(pathFull, function (err, stat) {
        if (err) {
          return debug(err);
        }

        // if directory, go deep...
        if (stat && stat.isDirectory()) {
          loadPaths(pathFull, function (err, res) {
            results = results.concat(res);
            if (!--pending) callback(null, results);
          });
          return;
        }

        // if a file we are looking for
        if (isValidFileType(pathFull)) {
          results.push(pathFull);
        }

        // of just an other file, skip...
        if (!--pending) callback(null, results);
      });
    });
  });
}

function pathToJson (root, paths, addWarning) {
  root = path.resolve(root);
  const res = [];

  paths.forEach(function (p) {
    if (p.indexOf(root) >= 0) {
      let part = p.substring(root.length);
      if (part.indexOf(path.sep) === 0) {
        part = part.substring(path.sep.length);
      }

      const splits = part.split(path.sep);
      const withoutFileName = splits;
      withoutFileName.splice(splits.length - 1);
      const fileName = path.basename(part);

      let dottiedBase = '';
      withoutFileName.forEach(function (s, i) {
        if (i + 1 < withoutFileName.length) {
          dottiedBase += s + '.';
        }
        else {
          dottiedBase += s;
        }
      });

      try {
        let required = require(p);

        //        // clean cache, fixes multiple loading of same aggregates, commands, etc...
        //        if (require.cache[require.resolve(p)]) {
        //          delete require.cache[require.resolve(p)];
        //        }

        if (!required || _.isEmpty(required)) {
          return;
        }

        if (typeof required === 'object' && typeof required.default !== 'undefined') {
          required = required.default;
        }

        if (_.isArray(required)) {
          _.each(required, function (req) {
            res.push({
              path: p,
              dottiedBase: dottiedBase,
              fileName: fileName,
              value: req,
              fileType: path.extname(p).substring(1),
            });
          });
        }
        else {
          res.push({
            path: p,
            dottiedBase: dottiedBase,
            fileName: fileName,
            value: required,
            fileType: path.extname(p).substring(1),
          });
        }
      }
      catch (err) {
        debug(err);
        if (addWarning) {
          addWarning(err);
        }
      }
    }
    else {
      debug('path is not a subpath from root');
    }
  });

  return res;
}

function parse (dir, filter, callback) {
  if (!callback) {
    callback = filter;
    filter = function (r) {
      return r;
    };
  }

  dir = path.resolve(dir);
  loadPaths(dir, function (err, paths) {
    if (err) {
      return callback(err);
    }

    let warns = [];
    function addWarning (e) {
      warns.push(e);
    }

    const res = filter(pathToJson(dir, paths, addWarning));

    const dottiesParts = [];

    res.forEach(function (r) {
      const parts = r.dottiedBase.split('.');
      parts.forEach(function (p, i) {
        if (!dottiesParts[i]) {
          return dottiesParts[i] = [p];
        }

        if (dottiesParts[i].indexOf(p) < 0) {
          dottiesParts[i].push(p);
        }
      });
    });

    let toRemove = '';

    for (let pi = 0, plen = dottiesParts.length; pi < plen; pi++) {
      if (dottiesParts[pi].length === 1) {
        toRemove += dottiesParts[pi][0];
      }
      else {
        break;
      }
    }

    if (toRemove.length > 0) {
      res.forEach(function (r) {
        if (r.dottiedBase === toRemove) {
          r.dottiedBase = '';
        }
        else {
          r.dottiedBase = r.dottiedBase.substring(toRemove.length + 1);
        }
      });
    }

    if (warns.length === 0) {
      warns = null;
    }

    callback(null, res, warns);
  });
}

module.exports = parse;
