var mysql          = require('../');
var Connection     = require('./Connection');
var EventEmitter   = require('events').EventEmitter;
var Util           = require('util');
var PoolConnection = require('./PoolConnection');
const { performance } = require('perf_hooks');
const CONNECTION_HOLD_TIMEOUT = 600000; // 10 minutes

module.exports = Pool;

Util.inherits(Pool, EventEmitter);
function Pool(options) {
  EventEmitter.call(this);
  this.config = options.config;
  this.config.connectionConfig.pool = this;

  this._acquiringConnections = [];
  this._allConnections       = [];
  this._freeConnections      = [];
  this._connectionQueue      = [];
  this._closed               = false;
  this._timeouts             = {};
  this._connectionMetadata   = {}; // Track connection acquisition metadata for monitoring
}

Pool.prototype.getConnection = function (cb) {

  if (this._closed) {
    var err = new Error('Pool is closed.');
    err.code = 'POOL_CLOSED';
    process.nextTick(function () {
      cb(err);
    });
    return;
  }

  var connection;
  var pool = this;

  // Capture call site for monitoring (similar to trace feature)
  var callSite = null;
  if (process.env.MYSQL_QUERY_LOGGING == 1) {
    callSite = new Error();
  }

  // Wrap callback to clear timeout and pass call site
  var wrappedCb = function(err, connection) {
    // Clear the 60-second warning timeout if it was set
    if (wrappedCb._waitTimeoutId) {
      clearTimeout(wrappedCb._waitTimeoutId);
      delete wrappedCb._waitTimeoutId;
    }

    cb(err, connection);
  };

  // Store callSite on wrappedCb so it can be accessed earlier
  wrappedCb._callSite = callSite;

  // Set 60-second warning timeout for slow connection requests
  if (process.env.MYSQL_QUERY_LOGGING == 1) {
    wrappedCb._waitTimeoutId = setTimeout(
      handleSlowConnectionRequest,
      60000,
      callSite,
      pool
    );
  }

  if (this._freeConnections.length > 0) {
    // if (process.env.MYSQL_QUERY_LOGGING==1){
    //   console.log("getConnection() there are available free connections!");
    // }
    connection = this._freeConnections.shift();
    this.acquireConnection(connection, wrappedCb);
    return;
  }

  if (this.config.connectionLimit === 0 || this._allConnections.length < this.config.connectionLimit) {
    // if (process.env.MYSQL_QUERY_LOGGING==1){
    //   console.log("getConnection() need to get a new connection");
    // }
    connection = new PoolConnection(this, { config: this.config.newConnectionConfig() });

    this._acquiringConnections.push(connection);
    this._allConnections.push(connection);

    connection.connect({timeout: this.config.acquireTimeout}, function onConnect(err) {
      spliceConnection(pool._acquiringConnections, connection);

      if (pool._closed) {
        err = new Error('Pool is closed.');
        err.code = 'POOL_CLOSED';
      }

      if (err) {
        pool._purgeConnection(connection);
        wrappedCb(err);
        return;
      }

      // Attach call site before handleGetConnection
      if (wrappedCb._callSite) {
        connection._acquisitionCallSite = wrappedCb._callSite;
      }

      if (process.env.MYSQL_QUERY_LOGGING==1){
        handleGetConnection(connection.threadId, pool._timeouts, connection);
      }

      pool.emit('connection', connection);
      pool.emit('acquire', connection);
      wrappedCb(null, connection);
    });
    return;
  }

  if (!this.config.waitForConnections) {
    process.nextTick(function(){
      var err = new Error('No connections available.');
      err.code = 'POOL_CONNLIMIT';
      wrappedCb(err);
    });
    return;
  }

  this._enqueueCallback(wrappedCb);
};

Pool.prototype.acquireConnection = function acquireConnection(connection, cb) {
  if (connection._pool !== this) {
    throw new Error('Connection acquired from wrong pool.');
  }

  var changeUser = this._needsChangeUser(connection);
  var pool       = this;

  this._acquiringConnections.push(connection);

  function onOperationComplete(err) {
    spliceConnection(pool._acquiringConnections, connection);

    if (pool._closed) {
      err = new Error('Pool is closed.');
      err.code = 'POOL_CLOSED';
    }

    if (err) {
      pool._connectionQueue.unshift(cb);
      pool._purgeConnection(connection);
      return;
    }

    if (changeUser) {
      pool.emit('connection', connection);
    }

    pool.emit('acquire', connection);

    // Attach call site before handleGetConnection
    if (cb._callSite) {
      connection._acquisitionCallSite = cb._callSite;
    }

    if (process.env.MYSQL_QUERY_LOGGING == 1) {
      handleGetConnection(connection.threadId, pool._timeouts, connection);
    }

    cb(null, connection);
  }

  if (changeUser) {
    // restore user back to pool configuration
    connection.config = this.config.newConnectionConfig();
    connection.changeUser({timeout: this.config.acquireTimeout}, onOperationComplete);
  } else {
    // ping connection
    connection.ping({timeout: this.config.acquireTimeout}, onOperationComplete);
  }
};
function handleGetConnection(threadId, timeouts, connection) {
  timeouts[threadId] = setTimeout(handleTimeout, CONNECTION_HOLD_TIMEOUT, threadId, connection);

  // Store metadata for monitoring
  connection._pool._connectionMetadata[threadId] = {
    threadId: threadId,
    acquiredAt: performance.now(),
    acquiredAtDate: new Date(),
    callSite: connection._acquisitionCallSite || null,
    state: 'active'
  };
}

function handleTimeout(threadId, connection) {
  console.error(`🚨 Connection leak detected for connectionID:${threadId} (held for >10 minutes)`);

  var callSite = connection._acquisitionCallSite;
  if (callSite) {
    console.error('Connection was acquired from:');
    console.error(formatStackTrace(callSite.stack));
  }

  delete connection._pool._timeouts[connection.threadId];

  // Auto-cleanup: rollback and destroy
  connection.rollback((rollbackErr) => {
    if (rollbackErr) {
      console.error(`Rollback failed for leaked connection ${threadId}:`, rollbackErr.message);
    }
    connection.destroy();
    console.error(`🔧 Leaked connection ${threadId} cleaned up (destroyed)`);

  });
}

function handleSlowConnectionRequest(callSite, pool) {
  if (process.env.MYSQL_QUERY_LOGGING == 1) {
    console.error('⚠️  SLOW CONNECTION REQUEST DETECTED (>60s wait)');
    console.error('Pool State:', {
      freeConnections: pool._freeConnections.length,
      allConnections: pool._allConnections.length,
      acquiringConnections: pool._acquiringConnections.length,
      queueLength: pool._connectionQueue.length,
      connectionLimit: pool.config.connectionLimit
    });

    if (callSite) {
      console.error('\nConnection request originated from:');
      console.error(formatStackTrace(callSite.stack));
    }

    // Debug: Log all connections and metadata
    console.error('\nDEBUG - All connections:');
    var allConns = pool._allConnections.map(function(conn) {
      return {
        threadId: conn.threadId,
        isFree: pool._freeConnections.indexOf(conn) !== -1,
        isAcquiring: pool._acquiringConnections.indexOf(conn) !== -1
      };
    });
    console.error(JSON.stringify(allConns, null, 2));
    console.error('DEBUG - Metadata keys: ' + JSON.stringify(Object.keys(pool._connectionMetadata)));
    console.error('DEBUG - Full metadata: ' + JSON.stringify(pool._connectionMetadata, null, 2));

    // Log which connections are currently held
    var heldConnections = pool.getHeldConnectionInfo();
    console.error('DEBUG - heldConnections.length: ' + heldConnections.length);
    if (heldConnections.length > 0) {
      console.error('\nCurrently held connections:');
      console.error(JSON.stringify(heldConnections, null, 2));
    } else {
      console.error('No held connections found (this is the bug!)');
    }
  }
}

function formatStackTrace(stack) {
  if (!stack) return '';

  // Remove first line (Error: at ...) and filter out internal frames
  var lines = stack.split('\n').slice(1);
  return lines
    .filter(function(line) {
      return !line.includes('Pool.js') &&
             !line.includes('PoolConnection.js') &&
             !line.includes('node_modules');
    })
    .slice(0, 10) // Limit to 10 most relevant frames
    .join('\n');
}

Pool.prototype.releaseConnection = function releaseConnection(connection) {

  if (this._acquiringConnections.indexOf(connection) !== -1) {
    // connection is being acquired
    return;
  }

  if (connection._pool) {
    if (connection._pool !== this) {
      throw new Error('Connection released to wrong pool');
    }

    if (process.env.MYSQL_QUERY_LOGGING==1){
      clearTimeout(connection._pool._timeouts[connection.threadId]);
      delete connection._pool._timeouts[connection.threadId];
      delete connection._pool._connectionMetadata[connection.threadId];
    }

    if (this._freeConnections.indexOf(connection) !== -1) {
      // connection already in free connection pool
      // this won't catch all double-release cases
      throw new Error('Connection already released');
    } else {
      // add connection to end of free queue
      this._freeConnections.push(connection);
      this.emit('release', connection);
    }
  }

  if (this._closed) {
    // empty the connection queue
    this._connectionQueue.splice(0).forEach(function (cb) {
      var err = new Error('Pool is closed.');
      err.code = 'POOL_CLOSED';
      process.nextTick(function () {
        cb(err);
      });
    });
  } else if (this._connectionQueue.length) {
    // get connection with next waiting callback
    this.getConnection(this._connectionQueue.shift());
  }
};

Pool.prototype.end = function (cb) {
  this._closed = true;

  if (typeof cb !== 'function') {
    cb = function (err) {
      if (err) throw err;
    };
  }

  var calledBack   = false;
  var waitingClose = 0;

  function onEnd(err) {
    if (!calledBack && (err || --waitingClose <= 0)) {
      calledBack = true;
      cb(err);
    }
  }

  while (this._allConnections.length !== 0) {
    waitingClose++;
    this._purgeConnection(this._allConnections[0], onEnd);
  }

  if (waitingClose === 0) {
    process.nextTick(onEnd);
  }
};

Pool.prototype.query = function (sql, values, cb) {

  var query = Connection.createQuery(sql, values, cb);

  

  if (!(typeof sql === 'object' && 'typeCast' in sql)) {
    query.typeCast = this.config.connectionConfig.typeCast;
  }

  if (this.config.connectionConfig.trace) {
    // Long stack trace support
    query._callSite = new Error();
  }

  if (process.env.MYSQL_QUERY_LOGGING == 1 && query._callback) {
    var originalCallback = query._callback;
    var querySql = query.sql;
    var threshold = parseInt(process.env.MYSQL_LARGE_RESULT_THRESHOLD, 10) || 5000;
    query._callback = function (err, results, fields) {
      if (!err && Array.isArray(results) && results.length > threshold) {
        var sqlUpper = (querySql || '').trimStart().substring(0, 6).toUpperCase();
        if (sqlUpper === 'SELECT') {
          console.warn('⚠️  Large result set warning: SELECT query returned ' + results.length + ' rows (threshold: ' + threshold + ')');
          console.warn('    Query: ' + (querySql || '').substring(0, 200));
        }
      }
      originalCallback.apply(this, arguments);
    };
  }

  this.getConnection(function (err, conn) {
    if (err) {
      query.on('error', function () {});
      query.end(err);
      return;
    }

    const start = performance.now();

    // Release connection based off event
    query.once('end', function() {
      conn.release();
    });
    conn.query(query);
  });

  return query;
};

Pool.prototype._enqueueCallback = function _enqueueCallback(callback) {

  if (this.config.queueLimit && this._connectionQueue.length >= this.config.queueLimit) {
    process.nextTick(function () {
      var err = new Error('Queue limit reached.');
      err.code = 'POOL_ENQUEUELIMIT';
      callback(err);
    });
    return;
  }

  // Bind to domain, as dequeue will likely occur in a different domain
  var cb = process.domain
    ? process.domain.bind(callback)
    : callback;

  this._connectionQueue.push(cb);
  this.emit('enqueue');
};

Pool.prototype._needsChangeUser = function _needsChangeUser(connection) {
  var connConfig = connection.config;
  var poolConfig = this.config.connectionConfig;

  // check if changeUser values are different
  return connConfig.user !== poolConfig.user
    || connConfig.database !== poolConfig.database
    || connConfig.password !== poolConfig.password
    || connConfig.charsetNumber !== poolConfig.charsetNumber;
};

Pool.prototype._purgeConnection = function _purgeConnection(connection, callback) {
  var cb = callback || function () {};

  if (connection.state === 'disconnected') {
    connection.destroy();
  }

  this._removeConnection(connection);

  if (connection.state !== 'disconnected' && !connection._protocol._quitSequence) {
    connection._realEnd(cb);
    return;
  }

  process.nextTick(cb);
};

Pool.prototype._removeConnection = function(connection) {
  connection._pool = null;

  // Remove connection from all connections
  spliceConnection(this._allConnections, connection);

  // Remove connection from free connections
  spliceConnection(this._freeConnections, connection);

  this.releaseConnection(connection);
};

Pool.prototype.escape = function(value) {
  return mysql.escape(value, this.config.connectionConfig.stringifyObjects, this.config.connectionConfig.timezone);
};

Pool.prototype.escapeId = function escapeId(value) {
  return mysql.escapeId(value, false);
};

Pool.prototype.getPoolMetrics = function() {
  var now = performance.now();
  var activeConnections = [];

  // Calculate active connections (not in free pool and not acquiring)
  this._allConnections.forEach(function(conn) {
    if (this._freeConnections.indexOf(conn) === -1 &&
        this._acquiringConnections.indexOf(conn) === -1) {

      var metadata = this._connectionMetadata[conn.threadId];
      if (metadata) {
        activeConnections.push({
          threadId: conn.threadId,
          heldDuration: Math.round(now - metadata.acquiredAt),
          acquiredAt: metadata.acquiredAtDate.toISOString(),
          callSite: metadata.callSite ? formatStackTrace(metadata.callSite.stack) : null
        });
      }
    }
  }, this);

  // Sort by held duration (longest first)
  activeConnections.sort(function(a, b) {
    return b.heldDuration - a.heldDuration;
  });

  return {
    timestamp: new Date().toISOString(),
    poolConfig: {
      connectionLimit: this.config.connectionLimit,
      queueLimit: this.config.queueLimit,
      waitForConnections: this.config.waitForConnections
    },
    connections: {
      total: this._allConnections.length,
      free: this._freeConnections.length,
      active: this._allConnections.length - this._freeConnections.length - this._acquiringConnections.length,
      acquiring: this._acquiringConnections.length
    },
    queue: {
      length: this._connectionQueue.length,
      waiting: this._connectionQueue.length
    },
    activeConnections: activeConnections,
    longHeldConnections: activeConnections.filter(function(c) {
      return c.heldDuration > 300000; // > 5 minutes
    })
  };
};

Pool.prototype.getHeldConnectionInfo = function() {
  return this.getPoolMetrics().activeConnections;
};

function spliceConnection(array, connection) {
  var index;
  if ((index = array.indexOf(connection)) !== -1) {
    // Remove connection from all connections
    array.splice(index, 1);
  }
}
