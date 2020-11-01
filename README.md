# sharedb-redis-db
In-memory database adapter with Redis for ShareDB. Just a clone of the implementation by [Win Min Tun](https://github.com/winmintun/).

Redis database adapter for [sharedb](https://github.com/share/sharedb). This
driver can be used both as a snapshot store and op log.


## Usage

`sharedb-redis` wraps [redis](https://www.npmjs.com/package/redis), and it supports the same configuration options. And it uses `RedLock` locking [redlock] (https://www.npmjs.com/package/redlock)

To instantiate a sharedb-redis, invoke the module and pass in your
Redis configuration and other options as an argument. For example:

```js
const redisOptions = {redis: {host: REDIS_HOST, port: REDIS_PORT}};
var RedisDB = require('sharedb-redis')(redisOptions);
var backend = require('sharedb')({db: RedisDB})
```

Can customise the prefixs to `ops` and `snapshots` keys (default is 's' and 'o')

```js
const redisOptions = {redis: {host: REDIS_HOST, port: REDIS_PORT},  keys: {snapshots: 'ss', ops: 'oo'}};
```

Can customise the redlock expiry ms (default 1000)

```js
const redisOptions = {redis: {host: REDIS_HOST, port: REDIS_PORT},  keys: {snapshots: 'ss', ops: 'oo'}, redlock: {expiry: 1000}};
```

Can customise the persistence event call interval in ms (default 60000)

```js
const redisOptions = {redis: {host: REDIS_HOST, port: REDIS_PORT},  keys: {snapshots: 'ss', ops: 'oo'}, redlock: {expiry: 1000}, persist: {interval: 60000}};
```

## Persistence

Redis mainly runs on memory and very fast. But for persistence, can go with Redis persistence option or by listening to the callback at predefined intervals to store somewhere else

Even though with persistence options (with redis persistence or the library support), Redis is not suitable to run with dataset larger than memory. The library keeps only 10 ops for each document at any time, so the only possibilities for memory overflow are the document size and no of documents.

```js
// Redis persistance call
RedisDB.persist = (snaps, ops, force) => {

    // if force === true when the persistence call is made due to `delete unused documents`

    // arrays of all the snaps and their ops for all collection/documents
	console.log(JSON.stringify(snap), 'snap');
    console.log(JSON.stringify(ops), 'ops');
};
```

## Delete unused documents

Redis runs on memory and to avoid memory overrun as far as we can, we need to clear unused documents (nobody is usng anymore for the time being). It will call persistence with force=true

```js
RedisDB.delete(collection, docId);
```

## Error codes

Redis & RedLock errors are passed back directly.

## Future @TODO

Need to support multiple independent Redis clusters and nodes

## MIT License

Copyright (c) 2017 by Win Min Tun

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

