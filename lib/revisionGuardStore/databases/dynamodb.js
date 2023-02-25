'use strict';

const debug = require('debug')('denormalizer:revisionGuardStore:dynamodb');

const async = require('async');
const _ = require('lodash');
const uuid = require('uuid').v4;
const aws = Store.use('aws-sdk');

const Store = require('denormalizer/lib/revisionGuardStore/base');
const ConcurrencyError = require('denormalizer/lib/errors/concurrencyError');


const collections = [];

class DynamoDB extends Store {
  constructor (options) {
    super(options);
    const awsConf = {
      region: 'ap-southeast-2',
      endpointConf: {},
    };

    if (process.env['AWS_DYNAMODB_ENDPOINT']) {
      awsConf.endpointConf = {endpoint: process.env['AWS_DYNAMODB_ENDPOINT']};
    }

    this.options = _.defaults(options, awsConf);

    const defaults = {
      tableName: 'revision',
      ReadCapacityUnits: 1,
      WriteCapacityUnits: 3,
    };

    this.options = _.defaults(this.options, defaults);
    this.store = {};
    this.lastEvent = null;
  }

  connect (callback) {
    const self = this;
    self.client = new aws.DynamoDB(self.options.endpointConf);
    const clientDefaults = {service: self.client};
    const clientOptions = _.defaults(this.options.clientConf, clientDefaults);
    self.documentClient = new aws.DynamoDB.DocumentClient(clientOptions);
    self.isConnected = true;
    self.emit('connect');
    if (callback) callback(null, self);
  }

  disconnect (callback) {
    this.emit('disconnect');
    if (callback) callback(null);
  }

  get (id, callback) {
    const self = this;

    if (_.isFunction(id)) {
      callback = id;
      id = null;
    }

    if (!id) {
      id = uuid().toString();
    }
    self.checkConnection(function (err) {
      if (err) {
        return callback(err);
      }

      const params = {
        TableName: self.options.tableName,
        Key: {
          HashKey: id,
          RangeKey: id,
        },
      };

      self.documentClient.get(params, function (err, data) {
        if (err) {
          if (callback) callback(err);
          return;
        }
        else {
          if (!data || !data.Item) {
            return callback(null, null);
          }
          callback(null, data.Item.Revision);
        }
      });
    });
  }

  set (id, revision, oldRevision, callback) {
    const self = this;

    if (!id || !_.isString(id)) {
      const err = new Error('Please pass a valid id!');
      debug(err);
      return callback(err);
    }
    if (!revision || !_.isNumber(revision)) {
      const err = new Error('Please pass a valid revision!');
      debug(err);
      return callback(err);
    }

    self.checkConnection(function (err) {
      if (err) {
        return callback(err);
      }
      const entity = {
        TableName: self.options.tableName,
        Item: {
          HashKey: id,
          RangeKey: id,
          Revision: revision,
        },
        ConditionExpression: 'attribute_not_exists(HashKey) OR Revision = :oldRevision',
        ExpressionAttributeValues: {
          ':oldRevision': oldRevision,
        },
      };
      self.documentClient.put(entity, function (err, data) {
        if (err) {
          if (err.code == 'ConditionalCheckFailedException') {
            err = new ConcurrencyError();
          }
          return callback(err);
        }
        return callback(err);
      });
    });
  }

  saveLastEvent (evt, callback) {
    const self = this;
    self.checkConnection(function (err) {
      if (err) {
        return callback(err);
      }
      const entity = {
        TableName: self.options.tableName,
        Item: {
          HashKey: 'THE_LAST_SEEN_EVENT',
          RangeKey: 'THE_LAST_SEEN_EVENT',
          event: evt,
        },
      };

      self.documentClient.put(entity, function (err, data) {
        if (err) {
          if (err.code == 'ConditionalCheckFailedException') {
            err = new ConcurrencyError();
          }
          return callback(err);
        }
        return callback(err);
      });
    });
  }

  getLastEvent (callback) {
    const self = this;
    self.checkConnection(function (err) {
      if (err) {
        return callback(err);
      }
      const params = {
        TableName: self.options.tableName,
        Key: {
          HashKey: 'THE_LAST_SEEN_EVENT',
          RangeKey: 'THE_LAST_SEEN_EVENT',
        },
      };

      self.documentClient.get(params, function (err, data) {
        if (err) {
          if (callback) callback(err);
          return;
        }
        else {
          if (!data || !data.Item) {
            return callback(null, null);
          }
          callback(null, data.Item.event);
        }
      });
    });
  }

  checkConnection (callback) {
    const self = this;

    if (collections.indexOf(self.collectionName) >= 0) {
      if (callback) callback(null);
      return;
    }

    createTableIfNotExists(
        self.client,
        revisionTableDefinition(self.options.tableName, self.options),
        function (err) {
          if (err) {
          // ignore ResourceInUseException
          // as there could be multiple requests attempt to create table concurrently
            if (err.code === 'ResourceInUseException') {
              return callback(null);
            }

            return callback(err);
          }

          if (collections.indexOf(self.collectionName) < 0) {
            collections.push(self.collectionName);
          }

          return callback(null);
        }
    );
  }

  clear (callback) {
    const self = this;
    self.checkConnection(function (err) {
      if (err) {
        return callback(err);
      }

      const query = {
        TableName: self.options.tableName,
      };
      self.documentClient.scan(query, function (err, entities) {
        if (err) {
          return callback(err);
        }
        async.each(
            entities.Items,
            function (entity, callback) {
              const params = {
                TableName: self.options.tableName,
                Key: {HashKey: entity.HashKey, RangeKey: entity.RangeKey},
              };
              self.documentClient.delete(params, function (error, response) {
                callback(error);
              });
            },
            function (error) {
              callback(error);
            }
        );
      });
    });
  }
}


const createTableIfNotExists = function (client, params, callback) {
  const exists = function (p, cbExists) {
    client.describeTable({TableName: p.TableName}, function (err, data) {
      if (err) {
        if (err.code === 'ResourceNotFoundException') {
          cbExists(null, {exists: false, definition: p});
        }
        else {
          cbExists(err);
        }
      }
      else {
        cbExists(null, {exists: true, description: data});
      }
    });
  };

  const create = function (r, cbCreate) {
    if (!r.exists) {
      client.createTable(r.definition, function (err, data) {
        if (err) {
          cbCreate(err);
        }
        else {
          cbCreate(null, {
            Table: {
              TableName: data.TableDescription.TableName,
              TableStatus: data.TableDescription.TableStatus,
            },
          });
        }
      });
    }
    else {
      cbCreate(null, r.description);
    }
  };

  const active = function (d, cbActive) {
    let status = d.Table.TableStatus;
    async.until(
        function () {
          return status === 'ACTIVE';
        },
        function (cbUntil) {
          client.describeTable({TableName: d.Table.TableName}, function (
              err,
              data
          ) {
            if (err) {
              cbUntil(err);
            }
            else {
              status = data.Table.TableStatus;
              setTimeout(cbUntil, 1000);
            }
          });
        },
        function (err, r) {
          if (err) {
            return cbActive(err);
          }
          cbActive(null, r);
        }
    );
  };

  async.compose(active, create, exists)(params, function (err, result) {
    if (err) callback(err);
    else callback(null, result);
  });
};

function revisionTableDefinition (tableName, opts) {
  return {
    TableName: tableName,
    KeySchema: [
      {AttributeName: 'HashKey', KeyType: 'HASH'},
      {AttributeName: 'RangeKey', KeyType: 'RANGE'},
    ],
    AttributeDefinitions: [
      {AttributeName: 'HashKey', AttributeType: 'S'},
      {AttributeName: 'RangeKey', AttributeType: 'S'},
    ],
    ProvisionedThroughput: {
      ReadCapacityUnits: opts.ReadCapacityUnits,
      WriteCapacityUnits: opts.WriteCapacityUnits,
    },
  };
}

module.exports = DynamoDB;
