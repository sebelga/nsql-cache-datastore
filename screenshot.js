// datastore.js
const Datastore = require('@google-cloud/datastore');
const NsqlCache = require('nsql-cache');
const dsAdapter = require('nsql-cache-datastore');

const datastore = new Datastore();
const db = dsAdapter(datastore);
const cache = new NsqlCache({ db });

module.exports = { datastore, cache };

...

// Use the google-cloud API just as usual
// but now... with a cache!

const { datastore } = require('./datastore');

const key = datastore.key(['Post', 123]);
datastore.get(key).then( ... );
