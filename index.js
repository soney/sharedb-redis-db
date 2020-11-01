/* 	
*	Redis-backed ShareDB (https://github.com/share/sharedb) database
*	With persistence option
*	Wraps https://www.npmjs.com/package/redis
*	@author Win Min Tun (sawrochelais@gmail.com)
*	@version 1.4.1
*/

var DB = require('sharedb').DB;
var rdis = require('redis');
var Redlock = require('redlock'); 
var redis;

// for redis distributed locking using RedLock algorithm
var redlock;

// redis keys
var KEY_PREFIX_SNAPSHOTS;
var KEY_PREFIX_OPS;

var REDIS_LOCK_EXPIRY = 1000;

var REDIS_PERSISTENCE_EVENT_INTERVAL = 60000;

function RedisDB(options) {
	if (!(this instanceof RedisDB)) return new RedisDB(options);
	DB.call(this, options);

	this.closed = false;

	redis = rdis.createClient(options.redis);

	let redLockSettings = options.redLock;
	let driftFactor = redLockSettings && redLockSettings.driftFactor ? redLockSettings.driftFactor : 0.01;
	let retryCount = redLockSettings && redLockSettings.retryCount ? redLockSettings.retryCount : 10;
	let retryDelay = redLockSettings && redLockSettings.retryDelay ? redLockSettings.retryDelay : 200;
	let retryJitter = redLockSettings && redLockSettings.retryJitter ? redLockSettings.retryJitter : 200;

	// @TODO: need to support multiple independent redis nodes or clusters
	redlock = new Redlock(
		// you should have one client for each independent redis node
		// or cluster
		[redis],
		{
			// the expected clock drift; for more details
			// see http://redis.io/topics/distlock
			driftFactor: driftFactor, // time in ms
	
			// the max number of times Redlock will attempt
			// to lock a resource before erroring
			retryCount:  retryCount,
	
			// the time in ms between attempts
			retryDelay:  retryDelay, // time in ms
	
			// the max time in ms randomly added to retries
			// to improve performance under high contention
			// see https://www.awsarchitectureblog.com/2015/03/backoff.html
			retryJitter:  retryJitter // time in ms
		}
	);

	redlock.on('clientError', function(err) {
		console.error(`RedisLock clientError: ${err} - ${new Date()}`);
	});

	if (options.keys) {
		KEY_PREFIX_SNAPSHOTS = options.keys.snapshots ? options.keys.snapshots : 's';
		KEY_PREFIX_OPS = options.keys.ops ? options.keys.ops : 'o';
	} else {
		KEY_PREFIX_SNAPSHOTS = 's';
		KEY_PREFIX_OPS = 'o';
	}

	if (options.redlock && options.redlock.expiry) {
		REDIS_LOCK_EXPIRY = options.redlock.expiry;
	}

	if (options.persist && options.persist.interval) {
		REDIS_PERSISTENCE_EVENT_INTERVAL = options.persist.interval;
	}

	// persistence callback at regular interval
	setInterval(() => {

		if (this.persist) {

			// get all the snaps and ops of all the docs/collections
			persist(this);

		}
		
	}, REDIS_PERSISTENCE_EVENT_INTERVAL)

	// delete document from redis
	this.delete = (collection, docId, callback) => {
		delDoc(this, collection, docId, callback);
	}
	
	
};

module.exports = RedisDB;

RedisDB.prototype = Object.create(DB.prototype);

RedisDB.prototype.close = function(callback) {
	this.closed = true;
	if (callback) callback();
};

function rollback(client, done) {
	client.query('ROLLBACK', function(err) {
		return done(err);
	})
}


RedisDB.prototype.getOpsToSnapshot = function(collection, id, from, snapshot, options, callback) {
	var to = snapshot.v;
	this.getOps(collection, id, from, to, options, callback);
};

// Persists an op and snapshot if it is for the next version. Calls back with
// callback(err, succeeded)
RedisDB.prototype.commit = (collection, id, op, snapshot, options, callback) => {

	let opsKey = KEY_PREFIX_OPS + ":" + collection + ":" + id;
	let snapsKey = KEY_PREFIX_SNAPSHOTS + ":" + collection + ":" + id;


	// RedLock the key
	redlock.lock(`locks:${opsKey}`, REDIS_LOCK_EXPIRY).then((lock) => {
		let redLocks = [];
		redLocks.push(lock);

		redlock.lock(`locks:${snapsKey}`, REDIS_LOCK_EXPIRY).then((lock2) => {

		redLocks.push(lock2);

			redis.get(`${opsKey}`, (err, replyStr) => { 
				if (err) {
					// release the locks
					redLocks.forEach(lock => {
						lock.unlock();
					});
					callback(err);
					return;
				}

				try {
					let maxVer = 0;
		
					let ops;

					if (replyStr) {
						ops = JSON.parse(replyStr);
		
						// check max version of ops
						maxVer = Object.keys(ops).reduce(function(a, b) {
							return Math.max(a, b);
						});
					}
		
					if (snapshot.v !== parseInt(maxVer) + 1) {

						// release the locks
						redLocks.forEach(lock => {
							lock.unlock();
						});
						callback(null, false);
						return;
					}
		
					// note `version` in ops table is the version of the corresponding snapshot, not the op version. op version in in `operation` json. ops ver starts at 0 while snapshot ver at 1
					
					let now = Math.round((new Date()).getTime() / 1000);
					let newOp = {'operation': op, 'ctime': now}
		
					// only save last 10, discard previous ops
					let newOps = {};
					
					if (ops) {
						for(let i=maxVer; i>=maxVer-8;i--) {
							newOps[i] = ops[i];
						}
					}
					
					newOps[snapshot.v] = newOp;

					// if new snapshot, insert, or it will update the version
					let newSnap = {
						type: snapshot.type,
						v: snapshot.v,
						data: snapshot.data,
						ctime: now,
						mtime: now
					};

					// transaction begins, queue the commands
					let multi = redis.multi();

					multi.set(`${opsKey}`, JSON.stringify(newOps))
					.set(`${snapsKey}`, JSON.stringify(newSnap))
					// commit
					// drains multi queue and runs atomically
					.exec((err, replies) => {

						// release the locks
						redLocks.forEach(lock => {
							lock.unlock();
						});

						if (err) {
							callback(error);
							return;
						}

						callback(null, true);

					});
			
				} catch(error) {
					// release the locks
					redLocks.forEach(lock => {
						lock.unlock();
					});
					callback(error);
					return;
				}

			});
		}).catch((err) => {
			
			// lock wait timeout
			// Redis uses optimistic locking, only option is to retry lock again, but better throw out the error, it should be very rare as each key pairs of snap and ops are tied to one document and redis is very fast
			callback(err);
			
		}); 
	}).catch((err) => {
		
		// lock wait timeout
		// Redis uses optimistic locking, only option is to retry lock again, but better throw out the error, it should be very rare as each key pairs of snap and ops are tied to one document and redis is very fast
		callback(err);
		
	}); 


};

// Get the named document from the database. The callback is called with (err,
// snapshot). A snapshot with a version of zero is returned if the docuemnt
// has never been created in the database.
RedisDB.prototype.getSnapshot = function(collection, id, fields, options, callback) {

	let key = KEY_PREFIX_SNAPSHOTS + ":" + collection + ":" + id;

	redlock.lock(`locks:${key}`, REDIS_LOCK_EXPIRY).then((lock) => {

		redis.get(`${key}`, (err, replyStr) => {
			if (err) {
				lock.unlock();
				callback(err);
				return;
			}

			let snapshot;
			if (replyStr) {
				try {
					let reply = JSON.parse(replyStr);
					snapshot = new RedisSnapshot(
						id,
						reply.v,
						reply.type,
						reply.data,
						undefined // TODO: metadata
					)
				} catch(error) { // JSON parse error

					lock.unlock();
					callback(error);
					return;
				}
			} else {
				snapshot = new RedisSnapshot(
					id,
					0,
					null,
					undefined,
					undefined
				)
			}


			lock.unlock();
			callback(null, snapshot);
		});

	})
	.catch((err) => {
		
		// lock wait timeout
		// Redis uses optimistic locking, only option is to retry lock again, but better throw out the error, it should be very rare as each key pairs of snap and ops are tied to one document and redis is very fast
		callback(err);
		
	});
	
};

// Get operations between [from, to) noninclusively. (Ie, the range should
// contain start but not end).
//
// If end is null, this function should return all operations from start onwards.
//
// The operations that getOps returns don't need to have a version: field.
// The version will be inferred from the parameters if it is missing.
//
// Callback should be called as callback(error, [list of ops]);

RedisDB.prototype.getOps = function(collection, id, from, to, options, callback) {
	from++; to++; // ops ver starts at 0 while snapshot ver at 1

	if (typeof callback !== 'function') throw new Error('Callback required');

	let key = KEY_PREFIX_OPS + ":" + collection + ":" + id;

	redlock.lock(`locks:${key}`, REDIS_LOCK_EXPIRY).then((lock) => {
		redis.get(`${key}`, (err, replyStr) => {

			if (err) {
				lock.unlock();
				callback(err);
				return;
			}
			try {

				let reply = JSON.parse(replyStr);
				let ops = [];
				// version >= from AND version < to
				if (to) {
					for (let i = from; i< to; i++) {
						if (reply[i]) {
							ops.push(reply[i].operation);
						}
					}
				} else {

					// check max version of ops
					maxVer = Object.keys(reply).reduce(function(a, b) {
						return Math.max(a, b);
					});
					// from to max version
					for (let i = from; i< maxVer; i++) {
						if (reply[i]) {
							ops.push(reply[i].operation);
						} else {
							break;
						}
					}
				}

				lock.unlock();
				callback(null, ops);

			} catch(error) {
				lock.unlock();
				callback(error);
				return;
			}
		});

	})
	.catch((err) => {
		// lock wait timeout
		// Redis uses optimistic locking, only option is to retry lock again, but better throw out the error, it should be very rare as each key pairs of snap and ops are tied to one document and redis is very fast
		callback(err);
	});
};

function RedisSnapshot(id, version, type, data, meta) {
	this.id = id;
	this.v = version;
	this.type = type;
	this.data = data;
	this.m = meta;
}

/**
 * Persistence
 * @param {*} RedisDBInstance 
 * @param {string} snapshot to persist particular snapshot only
 * @param {string} op to persist particular op only
 */
function persist(RedisDBInstance, snapshot, op, force=false) {

	if (!RedisDBInstance.persist) {
		return;
	}

	let searchSnapshot = snapshot ? snapshot: KEY_PREFIX_SNAPSHOTS;
	let searchOp = op ? op : KEY_PREFIX_OPS;

	return Promise.all([
		searchRedisKeys(searchSnapshot),
		searchRedisKeys(searchOp)
	])
	.then(snapsOpsKeys => { 
		let snapKeys = snapsOpsKeys[0];
		let opsKeys = snapsOpsKeys[1];

		if (snapKeys.length !== opsKeys.length) {
			// in case some keys are deleted accidentally
			return;
		}

		let finalSnaps = {};
		let finalOps = {};

		new Promise((resolve, reject) => {
			// lock snaps and ops key by document wise
			// to minimize interruption
			for(let i=0; i < snapKeys.length; i++) {
				let docSnapKey = snapKeys[i]; // each document snap key
				let docOpsKey = opsKeys[i]; // each document ops key

				redlock.lock(`locks:${docSnapKey}`, REDIS_LOCK_EXPIRY).then((lock) => {
					let redLocks = [];
					redLocks.push(lock);
			
					redlock.lock(`locks:${docOpsKey}`, REDIS_LOCK_EXPIRY).then((lock2) => {
			
						redLocks.push(lock2);

						redis.get(`${docOpsKey}`, (err, replyStr) => { 
							if (err || !replyStr) {
								// release the locks
								redLocks.forEach(lock => {
									lock.unlock();
								});
								return;
							}

							let docOps;
							try {
								docOps = JSON.parse(replyStr);
							} catch(e) { // parse exception
								// release the locks
								redLocks.forEach(lock => {
									lock.unlock();
								});
								return;
							}
							

							redis.get(`${docSnapKey}`, (err, replyStr) => { 
								if (err || !replyStr) {
									// release the locks
									redLocks.forEach(lock => {
										lock.unlock();
									});
									return;
								}
		
								let docSnap;
								try {
									docSnap = JSON.parse(replyStr);
								} catch(e) { // parse exception
									// release the locks
									redLocks.forEach(lock => {
										lock.unlock();
									});
									return;
								}
								

								
								// add doc id and collection
								docSnap.collection = getCollectionFromKey(docOpsKey);
								docSnap.doc = getDocFromKey(docOpsKey);

								let docOpsArray = [];
								Object.keys(docOps).forEach((key) => {
									let op = {
										collection: getCollectionFromKey(docSnapKey),
										doc: getDocFromKey(docSnapKey),
										v: key,
										operation: docOps[key].operation,
										ctime: docOps[key].ctime
									};

									docOpsArray.push(op);
								});

								finalOps[getDocFromKey(docSnapKey)] = docOpsArray;
								finalSnaps[getDocFromKey(docOpsKey)] = docSnap;

								// unlock
								redLocks.forEach(lock => {
									lock.unlock();
								});

								if (snapKeys.length == Object.keys(finalSnaps).length) {
									resolve();
								}
							});


						});
		
						
		
					});
		
				});
			}

		}).then(() => {
			// call persistence callback
			RedisDBInstance.persist(finalSnaps, finalOps, force);
		})
		
	})
	.catch(err => {
		
	})
}

function searchRedisKeys(needle) {
	
	return new Promise((resolve, reject) => {
		redis.keys(`*${needle}*`, (err, reply) => {
			if (err) {
				reject(err)
			}
			resolve(reply.sort());
		})
	});
		
}

function getCollectionFromKey(key) {
	let collection = null;
	let parts = key.split(':');
	
	if (parts.length > 1) {
		collection = parts[parts.length - 2];
	}

	return collection;
}

function getDocFromKey(key) {
	let doc = null;
	
	let parts = key.split(':');

	if (parts.length > 1) {
		doc = parts[parts.length - 1];
	}

	return doc;
}

// Delete document (snap and ops) on redis
function delDoc(RedisDBInstance, collection, docId, callback) {

	let snapKey = KEY_PREFIX_SNAPSHOTS + ":" + collection + ":" + docId;
	let opsKey = KEY_PREFIX_OPS + ":" + collection + ":" + docId;

	// persist the snap and op first before delete
	// 'true' means force persist, not interval persistence
	persist(RedisDBInstance, snapKey, opsKey, true)
	.then(() => {
		let locks = [];

		redlock.lock(`locks:${snapKey}`, REDIS_LOCK_EXPIRY).then((lock) => {
			locks.push(lock);

			redlock.lock(`locks:${opsKey}`, REDIS_LOCK_EXPIRY).then((lock) => {
				locks.push(lock);

				redis.del(`${snapKey}`, (err, replyStr) => {

					if (err) {
						locks.forEach(each => {
							each.unlock();
						});
						
						if (callback) {
							callback(err);
						}
						
						return;
					}

					redis.del(`${opsKey}`, (err, replyStr) => {

						if (err) {
							locks.forEach(each => {
								each.unlock();
							});

							if (callback) {
								callback(err);
							}

							return;
						}

						if (callback) {
							callback();
						}
			
					});
		
				});
			});

		});
	});

}