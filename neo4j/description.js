'use strict';

var opts = {maxSockets: 25, keepAlive: true, keepAliveMsecs: 1000};
var Agent = require('http').Agent;
var neo4j = require('neo4j');

function _id(value) { return parseInt(value.substring(1)); }

module.exports = {
  name: 'Neo4J',

  startup: function (host, cb) {
    var db = new neo4j.GraphDatabase({
      url: 'http://neo4j:abc@' + host + ':7474',
      agent: new Agent(opts)});

    cb(db);
  },

 warmup: function (db, cb) {
    db.cypher({query:'MATCH (n:PROFILES) WITH n, n._key as key, n.AGE as age MATCH (n)--() return count(*) as count'},
      function (err, result) {
        if (err) return cb(err);

        console.log('INFO warmup done, relationships '+result.count);

        cb(null);
      }
    );
  },

  getCollection: function (db, name, cb) {
    cb(null, name.toUpperCase());
  },

  dropCollection: function (db, name, cb) {
    name = name.toUpperCase();

    db.cypher({query: 'MATCH (n:' + name + ') DELETE n'}, cb);
  },

  createCollection: function (db, name, cb) {
    cb();
  },

  getDocument: function (db, coll, id, cb) {
    db.cypher({query: 'MATCH (f:' + coll + ' {_key:{key}}) RETURN f',
               params: {key: _id(id)},
               headers: {Connection: 'keep-alive'},
               lean: true}, cb);
  },

  saveDocument: function (db, coll, doc, cb) {
    db.cypher({query: 'CREATE (f:' + coll + ' {doc})',
               params: {doc: doc},
               headers: {Connection: 'keep-alive'},
               lean: true}, cb);
  },

  aggregate: function (db, coll, cb) {
    db.cypher({query: 'MATCH (f:' + coll + ') RETURN f.AGE, count(*)',
               headers: {Connection: 'keep-alive'},
               lean: true},

              function (err, result) {
                if (err) return cb(err);

                cb(null, result.length);
              });
  },

  neighbors: function (db, collP, collR, id, i, cb) {
    db.cypher({query: 'MATCH (s:' + collP + ' {_key:{key}})-->(n) RETURN n._key',
               params: {key: _id(id)},
               headers: {Connection: 'keep-alive'},
               lean: true},

              function (err, result) {
                if (err) return cb(err);

                if (result.length === undefined) cb(null, 1);
                else cb(null, result.length);
              });
  },

  neighbors2: function (db, collP, collR, id, i, cb) {
    db.cypher({query: 'MATCH (s:' + collP + ' {_key:{key}})-->(m) MATCH (m)-->(n) RETURN distinct n._key',
               params: {key: _id(id)},
               headers: {Connection: 'keep-alive'},
               lean: true},

              function (err, result) {
                if (err) return cb(err);

                if (result.map === undefined) {
                  result = [result['n._key']];
                }
                else {
                  result = result.map(function (x) { return x['n._key']; });
                }

                if (result.indexOf(id) === -1) {
                  cb(null, result.length);
                }
                else {
                  cb(null, result.length - 1);
                }
              });
  },

  shortestPath: function (db, collP, collR, path, i, cb) {
    db.cypher({query: 'MATCH (s:' + collP + ' {_key:{from}}),(t:'
                      + collP + ' {_key:{to}}) MATCH p = shortestPath((s)-[*..15]->(t)) RETURN [n in nodes(p) | id(n)] as p',
               params: {from: _id(path.from), to: _id(path.to)},
               headers: {Connection: 'keep-alive'},
               lean: true},

              function (err, result) {
                if (err) return cb(err);

		if (result.length === 0) {cb(null, 0);}
                else {cb(null, (result[0].p.length - 1) / 2);}
              });
  }
};
