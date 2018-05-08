'use strict';

/* eslint-disable no-param-reassign */

/**
 * |-----------------------------------------------------|
 * |                                                     |
 * |      GOOGLE DATASTORE ADAPTER for nsql-cache        |
 * |                                                     |
 * |-----------------------------------------------------|
 */

const arrify = require('arrify');

const isCacheOn = (options, config) => {
    if (typeof options.cache !== 'undefined') {
        return options.cache;
    }
    return config.global !== false;
};

const separator = ':%:';

module.exports = ds => {
    const datastoreAdapter = {
        /**
         * Get one or multiple (array) entities by Key
         *
         * @param {any} keys Database key
         * @returns Promise resolving the entities data
         */
        getEntity(keys) {
            return ds.get(keys);
        },
        /**
         * Extract the key from an entity
         * This method is optional for database adapters
         * The Datastore keeps the Key in a Symbol on the object
         *
         * @param {any} entity entity returned by the database
         * @returns the entity Key
         */
        getKeyFromEntity(entity) {
            if (!entity) {
                return entity;
            }
            return entity[ds.KEY];
        },
        /**
         * Read the entity Kind from a Datastore Query object
         *
         * @param {any} query Datastore query object
         * @returns {string} the entity Kind
         */
        getEntityKindFromQuery(query) {
            return query.kinds[0];
        },
        /**
         * Add the entity Key to its data object
         * This is how nsql-cache can put back the Symbol on the entity
         * after the entity has been returned from the cache
         *
         * @param {any} key The entity Key
         * @param {any} entity The entity data
         * @returns The entity with the key
         */
        addKeyToEntity(key, entity) {
            if (!entity) {
                return entity;
            }
            return Object.assign({}, entity, { [ds.KEY]: key });
        },
        /**
         * Convert a Google Datastore Key to a unique string id
         *
         * @param {any} key Datastore Key
         * @returns  {string} a unique string representing the key
         */
        keyToString(key) {
            if (typeof key === 'undefined') {
                throw new Error('Key cannot be undefined.');
            }
            let str = key.namespace ? key.namespace + separator : '';
            str += key.path.join(separator);
            return str;
        },
        /**
         * Convert a Google Datastore Query to a unique string id
         *
         * @param {any} query A Datastore Query object
         * @returns {string} a unique string representing the query
         */
        queryToString(query) {
            const array = [];
            array.push(query.kinds.join(separator));
            array.push(query.namespace);
            array.push(
                query.filters.reduce((acc, filter) => {
                    let str = acc + filter.name + filter.op;

                    // When filtering with "hancestors"
                    // the value is a Datastore Key.
                    // we need to parse it as well
                    if (ds.isKey(filter.val)) {
                        str += datastoreAdapter.keyToString(filter.val, { hash: false });
                    } else {
                        str += filter.val;
                    }
                    return str;
                }, '')
            );

            array.push(query.groupByVal.join(''));
            array.push(query.limitVal);
            array.push(query.offsetVal);
            array.push(query.orders.reduce((acc, order) => acc + order.name + order.sign, ''));
            array.push(query.selectVal.join(''));
            array.push(query.startVal);
            array.push(query.endVal);

            return array.join(separator);
        },
        /**
         * Default handler to execute a query
         *
         * @param {any} query Datastore query
         * @returns A Promise resolving the query
         */
        runQuery(query) {
            return query.run();
        },
        /**
         * Wrap the @google-cloud/datastore instance
         * to automatically manage the cache when an entity
         * is added, modified or deleted.
         *
         * @param {any} cache The NsqlCache instance
         */
        wrapClient(cache) {
            /**
             * With the "wrapClient" method we add an abstraction layer on top of the
             * @google/datastore client methods. This layer automatically manages the cache
             * so an application can benefit from it without modifying the implementation code.
             */
            datastoreAdapter.wrapped = true;

            /**
             * Wrap datastore.get()
             */
            const originalGet = ds.get.bind(ds);
            /**
             * We keep a reference to the "unwrapped" client method to fetch an entity
             * This allows gstore-cache to support both scenario
             *
             * 1. Using the cache with the wrapped method
             * datastore.get(key) -> gsCache.read(key) -> originalClient.get(key)
             *
             * 2. Using the cache calling the "keys.read()"
             * To avoid to call twice the gsCache.read
             * gsCache.read(key) -> datastore.get(key) -> gsCache.read(key) -> originalClient.get(key)
             * We add the reference to the unWrapped method (see keys.js fetchHandler creation)
             * gsCache.read(key) -> gsCache.db.getEntityUnwrapped(key)
             */
            datastoreAdapter.getEntityUnWrapped = originalGet;
            ds.get = (keys, options = {}) => {
                if (!isCacheOn(options, cache.config)) {
                    return originalGet(keys);
                }
                return cache.keys.read(keys, options.cache, originalGet).then(res => arrify(res));
            };

            /**
             * Wrap datastore.createQuery()
             */
            const { createQuery } = ds;
            ds.createQuery = (...args) => {
                const query = createQuery.call(ds, ...args);
                // Keep a ref to the original "query.run()" method
                const originalRun = query.run.bind(query);
                datastoreAdapter.runQueryUnWrapped = originalRun;
                query.run = (options = {}) => {
                    if (!isCacheOn(options, cache.config)) {
                        return originalRun(options);
                    }
                    return cache.queries.read(query, options.cache, () => originalRun(options));
                };
                return query;
            };

            /**
             * Wrap datastore.save()
             *
             * When we save an entity, we need to
             * - prime the cache with the entity data just saved
             * - clear all queries linked to the Entity Kind just saved
             */
            const originalSave = ds.save.bind(ds);
            ds.save = (entities, options = {}) =>
                originalSave(entities, options).then(res => {
                    if (!isCacheOn(options, cache.config)) {
                        return res;
                    }

                    let keysValues;
                    const isMultiple = Array.isArray(entities);
                    if (isMultiple) {
                        keysValues = [].concat(...entities.map(({ key, data }) => [key, data]));
                    }

                    return Promise.all([
                        isMultiple
                            ? cache.keys.mset(...keysValues, options.cache)
                            : cache.keys.set(entities.key, entities.data, options.cache),
                        isMultiple
                            ? cache.queries.clearQueriesByKind(entities.map(({ key }) => key.kind))
                            : cache.queries.clearQueriesByKind(entities.key.kind),
                    ]).then(() => res);
                });

            ds.update = (entities, options) => {
                entities = arrify(entities);
                return ds.save(entities.map(e => Object.assign({}, e, { method: 'update' })), options);
            };

            ds.insert = (entities, options) => {
                entities = arrify(entities);
                return ds.save(entities.map(e => Object.assign({}, e, { method: 'insert' })), options);
            };

            ds.upsert = (entities, options) => {
                entities = arrify(entities);
                return ds.save(entities.map(e => Object.assign({}, e, { method: 'upsert' })), options);
            };

            /**
             * Wrap datastore.delete()
             * When we delete an entity, we need to
             * - delete the cache for that entity
             * - clear all queries linked to the Entity Kind just deleted
             */
            const originalDelete = ds.delete.bind(ds);
            ds.delete = keys =>
                originalDelete(keys).then(res => {
                    const isMultiple = Array.isArray(keys);
                    return Promise.all([
                        isMultiple ? cache.keys.del(...keys) : cache.keys.del(keys),
                        isMultiple
                            ? cache.queries.clearQueriesByKind(keys.map(key => key.kind))
                            : cache.queries.clearQueriesByKind(keys.kind),
                    ]).then(() => res);
                });
        },
    };

    return datastoreAdapter;
};
