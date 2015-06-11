'use strict';

var http = require('http');

function _id(v) { return parseInt(v.substring(1)); }

module.exports = {
  name: 'Neo4J',

// new neo4j.GraphDatabase('http://username:passwordlocalhost:7474');
  startup: function (host, cb) {
	var user = "neo4j";
	var pass = "abc";
    var neo4j = require('neo4j');
    var db = new neo4j.GraphDatabase({url:'http://'+user+':' + pass + '@'+ host + ':7474',agent:new http.Agent({maxSockets:10})});
    cb(db);
  },

  warmup: function (db, cb) {

    db.cypher({query:'MATCH (:PROFILES) return count(*) as count'},
      function (err, result) {
        if (err) return cb(err);

        console.log('INFO warmup done, relationships '+result[0].count);

        cb(null);
      }
    );
  },

  getCollection: function (db, name, cb) {
    cb(null, name.toUpperCase());
  },

  dropCollection: function (db, coll, cb) {
    coll = coll.toUpperCase();
	var deleted = 0;
	function deleteByLabel() {
	    db.cypher({query:'MATCH (n:' + coll + ') WITH n LIMIT 5000 OPTIONAL MATCH (n)-[r]-() DELETE n,r RETURN count(*) as deleted'},
	      function (err, result) {
	        if (err) return cb(err);
			if (result.length && result[0].deleted > 0) {
				 deleted += result[0].deleted;
				 deleteByLabel();
		    }
	        else cb(null, deleted);
	      });
	}
	deleteByLabel();
  },

  createCollection: function (db, name, cb) {
    cb(null, name.toUpperCase());
  },

  getDocument: function (db, coll, id, cb) {
	db.cypher({query:'MATCH (n:'+coll+' {_key: {key}}) RETURN n',params:{key: _id(id)}},
	    function (err, result) {
	      if (err) return cb(err);
	
	      cb(null, result.length ? result[0].n : 0);
	    }
	);
  },

  saveDocument: function (db, coll, doc, cb) {
	var failed = 0;
	db.cypher({query:'CREATE (n:'+coll+' {data}) RETURN id(n) as id',params:{data:doc}},
	    function (err, result) {
	      if (err) {
		    failed++;
		    console.log("saveDocument ",failed,err);
			return setTimeout(cb,10);
		  }
	      cb(null, result[0].id);
	    }
	);
  },

  aggregate: function (db, coll, cb) {
    db.cypher({query:'MATCH (f:' + coll + ') RETURN f.AGE as AGE, count(*)'},
      function (err, result) {
        if (err) return cb(err);

        cb(null, result.length);
      }
    );
  },

  neighbors: function (db, collP, collR, id, i, cb) {
    db.cypher({query:'MATCH (s:' + collP + ' {_key:{key}})-->(n:' + collP + ') RETURN n._key', params: {key: _id(id)}},
      function (err, result) {
        if (err) return cb(err);

        if (result.length === undefined) cb(null, 1);
        else cb(null, result.length);
      }
    );
  },

  neighbors2: function (db, collP, collR, id, i, cb) {
    db.cypher({query:'MATCH (s:' + collP + ' {_key:{key}})-->(x) MATCH (x)-->(n) RETURN n._key', params: {key: _id(id)}},
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
      }
    );
  },

  neighbors3: function (db, collP, collR, id, i, cb) {
    db.cypher({query:'MATCH (s:' + collP + ' {_key:{key}})-[*1..3]->(n:' + collP + ') RETURN DISTINCT n._key',params: {key: _id(id)}},
      function (err, result) {
        if (err) return cb(err);

        result = result.map(function (x) { return x['n._key']; });

        if (result.indexOf(id) === -1) {
          cb(null, result.length);
        }
        else {
          cb(null, result.length - 1);
        }
      }
    );
  },

  shortestPath: function (db, collP, collR, path, i, cb) {
    db.cypher({query:'MATCH (s:' + collP + ' {_key:{from}}),(t:' + collP + ' {_key:{to}}), p = shortestPath((s)-[*..15]->(t)) RETURN [x in nodes(p) | x._key] as path',
      params:{from: _id(path.from), to: _id(path.to)}},
      function (err, result) {
        if (err) return cb(err);

        cb(null, result.length && result[0].path ? result[0].path.length : 0);
      }
    );
  }
};
