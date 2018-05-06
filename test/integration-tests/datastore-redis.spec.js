'use strict';

const Datastore = require('@google-cloud/datastore');
const chai = require('chai');
const sinon = require('sinon');
const redisStore = require('cache-manager-redis-store');
const NsqlCache = require('nsql-cache');
const { argv } = require('yargs');

const dsAdapter = require('../../lib');
const { string } = require('../utils');

const ds = new Datastore({ projectId: 'gstore-cache-integration-tests' });
const dsWrapped = new Datastore({ projectId: 'gstore-cache-integration-tests' });

const { expect, assert } = chai;

const allKeys = [];

// Helper to add some timeout between saving the entity and retrieving it
const addTimeOut = () => new Promise(resolve => setTimeout(resolve, 500));

describe('Integration Tests (Datastore & Memory + Redis cache)', () => {
    let cache;
    let redisClient;
    let queryToString;

    beforeEach(function integrationTest() {
        if (argv.integration !== true) {
            // Skip e2e tests suite
            this.skip();
        }

        cache = new NsqlCache({
            stores: [{ store: 'memory' }, { store: redisStore }],
            db: dsAdapter(ds),
            config: {
                wrapClient: false,
            },
        });

        const prefix = cache.config.cachePrefix.queries;
        queryToString = q => prefix + cache.db.queryToString(q);
        ({ redisClient } = cache);
    });

    afterEach(done => {
        redisClient.flushdb(() => done());
    });

    describe('NsqlCache.keys', () => {
        beforeEach(function TestNsqlCacheQueries() {
            if (argv.integration !== true) {
                // Skip e2e tests suite
                this.skip();
            }
        });

        it('should add the entities to cache', () => {
            const { store } = cache.stores[1];
            sinon.spy(store, 'mset');
            const key1 = ds.key(['User', string.random()]);
            const key2 = ds.key(['User', string.random()]);
            const entityData1 = { name: string.random() };
            const entityData2 = { name: string.random() };

            return cache.keys
                .mset(key1, entityData1, key2, entityData2, { ttl: { memory: 1122, redis: 3344 } })
                .then(result => {
                    const { args } = store.mset.getCall(0);
                    expect(args[4].ttl).equal(3344);

                    expect(result[0]).deep.equal(entityData1);
                    expect(result[1]).deep.equal(entityData2);
                });
        });
    });

    describe('NsqlCache.queries', () => {
        beforeEach(function TestNsqlCacheQueries() {
            if (argv.integration !== true) {
                // Skip e2e tests suite
                this.skip();
            }
        });

        describe('set()', () => {
            it('should add query data to Redis Cache + EntityKind Set', () => {
                sinon.spy(redisClient, 'multi');

                const q = ds.createQuery('User').filter('name', string.random());
                const entity = Object.assign({}, { a: 123 }, { [ds.KEY]: { id: 123 } });
                const response = [entity];

                return cache.queries.get(q).then(result1 => {
                    assert.isUndefined(result1); // make sure the cache is empty
                    return cache.queries
                        .set(q, response, { ttl: { memory: 1122, redis: 3344 } })
                        .then(() => {
                            const args = redisClient.multi.getCall(0).args[0];
                            expect(args[1]).contains('setex');
                            expect(args[1]).contains(3344);
                            redisClient.multi.restore();

                            return new Promise((resolve, reject) => {
                                redisClient.get(queryToString(q), (err, data) => {
                                    if (err) {
                                        return reject(err);
                                    }
                                    const cacheResponse = JSON.parse(data);
                                    const [entities] = cacheResponse;
                                    expect(entities[0]).contains(entity);

                                    // Make sure we saved the KEY Symbol
                                    assert.isDefined(entities[0].__dsKey__);
                                    return resolve();
                                });
                            });
                        })
                        .then(() =>
                            cache.queries.get(q).then(cacheResponse => {
                                const [entities] = cacheResponse;
                                // Make sure we put back from the Cache the Symbol
                                assert.isDefined(entities[0][ds.KEY]);
                                expect(entities[0][ds.KEY].id).equal(123);
                            })
                        )
                        .then(
                            () =>
                                new Promise((resolve, reject) => {
                                    redisClient.scard('gcq:User', (err, total) => {
                                        if (err) {
                                            return reject(err);
                                        }
                                        expect(total).equal(1);
                                        return resolve();
                                    });
                                })
                        );
                });
            });
        });

        describe('mget() & mset()', () => {
            it('should set and return multiple queries', () => {
                const q1 = ds.createQuery('User').filter('name', string.random());
                const q2 = ds.createQuery('User').filter('name', string.random());
                const entity1 = Object.assign({}, { a: 123 }, { [ds.KEY]: { id: 123 } });
                const entity2 = Object.assign({}, { a: 456 }, { [ds.KEY]: { id: 123 } });
                const resQuery1 = [entity1];
                const resQuery2 = [entity2];

                return cache.queries.mget(q1, q2).then(result1 => {
                    assert.isUndefined(result1[0]);
                    assert.isUndefined(result1[1]);

                    return cache.queries
                        .mset(q1, resQuery1, q2, resQuery2, { ttl: 600 })

                        .then(result2 => {
                            expect(result2[0]).deep.equal(resQuery1);
                            expect(result2[1]).deep.equal(resQuery2);

                            return cache.queries.mget(q1, q2);
                        })
                        .then(result3 => {
                            const [users] = result3[0];
                            const [posts] = result3[1];
                            expect(users).deep.equal([entity1]);
                            expect(posts).deep.equal([entity2]);
                        });
                });
            });
        });

        describe('read()', () => {
            it('should add query data to Redis Cache + EntityKind Set', () => {
                const userName = string.random();
                const entityName = string.random();
                const q = ds.createQuery('User').filter('name', userName);
                const key = ds.key(['User', entityName]);
                allKeys.push(key);
                const entityData = { name: userName };

                return cache.queries.get(q).then(result1 => {
                    assert.isUndefined(result1); // make sure the cache is empty
                    return ds
                        .save({ key, data: entityData })
                        .then(addTimeOut)
                        .then(() =>
                            cache.queries
                                .read(q)
                                .then(result2 => {
                                    const [entities] = result2;
                                    assert.isDefined(entities[0][ds.KEY]);
                                })
                                .then(
                                    () =>
                                        new Promise((resolve, reject) => {
                                            redisClient.get(queryToString(q), (err, data) => {
                                                if (err) {
                                                    return reject(err);
                                                }
                                                const cacheData = JSON.parse(data);
                                                const [entities, meta] = cacheData;
                                                expect(entities[0]).contains(entityData);
                                                assert.isDefined(meta.endCursor);

                                                // Make sure we saved the KEY Symbol
                                                assert.isDefined(entities[0].__dsKey__);
                                                return resolve();
                                            });
                                        })
                                )
                                .then(() =>
                                    cache.queries.get(q).then(cacheData => {
                                        const [entities] = cacheData;
                                        // Make sure we put back from the Cache the Symbol
                                        assert.isDefined(entities[0][ds.KEY]);
                                        expect(entities[0][ds.KEY].name).equal(entityName);
                                    })
                                )
                                .then(
                                    () =>
                                        new Promise((resolve, reject) => {
                                            redisClient.scard('gcq:User', (err, total) => {
                                                if (err) {
                                                    return reject(err);
                                                }
                                                expect(total).equal(1);
                                                return resolve();
                                            });
                                        })
                                )
                        );
                });
            });
        });

        describe('kset()', () => {
            const queryKey = 'my-query-key';
            const queryData = [{ id: 1, title: 'Post title', author: { name: 'John Snow' } }];

            it('should add query data to Redis Cache with multiple Entity Kinds', () =>
                cache.get(queryKey).then(result1 => {
                    assert.isUndefined(result1); // make sure the cache is empty
                    return cache.queries
                        .kset(queryKey, queryData, ['Post', 'Author'])
                        .then(
                            () =>
                                new Promise((resolve, reject) => {
                                    redisClient.get(queryKey, (err, data) => {
                                        if (err) {
                                            return reject(err);
                                        }
                                        return resolve(JSON.parse(data));
                                    });
                                })
                        )
                        .then(result2 => {
                            expect(result2).deep.equal(queryData);
                        })
                        .then(
                            () =>
                                new Promise((resolve, reject) => {
                                    redisClient.scard('gcq:Post', (err, total) => {
                                        if (err) {
                                            return reject(err);
                                        }
                                        expect(total).equal(1);
                                        return resolve();
                                    });
                                })
                        )
                        .then(
                            () =>
                                new Promise((resolve, reject) => {
                                    redisClient.scard('gcq:Author', (err, total) => {
                                        if (err) {
                                            return reject(err);
                                        }
                                        expect(total).equal(1);
                                        return resolve();
                                    });
                                })
                        );
                }));
        });

        describe('clearQueriesByKind()', () => {
            it('should delete cache and remove from EntityKind Set', () => {
                const userName = string.random();
                const q = ds.createQuery('User').filter('name', userName);
                const entity = Object.assign({}, { name: userName }, { [ds.KEY]: { id: 123 } });
                const queryRes = [entity];

                return cache.queries.get(q).then(result1 => {
                    assert.isUndefined(result1); // make sure the cache is empty
                    return cache.queries
                        .set(q, queryRes)
                        .then(
                            () =>
                                // Check that the Set does contains the Query
                                new Promise((resolve, reject) => {
                                    redisClient.scard('gcq:User', (err, total) => {
                                        if (err) {
                                            return reject(err);
                                        }
                                        expect(total).equal(1);
                                        return resolve();
                                    });
                                })
                        )
                        .then(() => cache.queries.clearQueriesByKind('User'))
                        .then(
                            () =>
                                // Check that Query Cache does not exist anymore
                                new Promise((resolve, reject) => {
                                    redisClient.get(queryToString(q), (err, data) => {
                                        if (err) {
                                            return reject(err);
                                        }
                                        expect(data).equal(null);
                                        return resolve();
                                    });
                                })
                        )
                        .then(
                            () =>
                                // Check that the Set does not contains any more Queries
                                new Promise((resolve, reject) => {
                                    redisClient.scard('gcq:User', (err, total) => {
                                        if (err) {
                                            return reject(err);
                                        }
                                        expect(total).equal(0);
                                        return resolve();
                                    });
                                })
                        );
                });
            });

            it('should delete cache and remove from multiple EntityKind Set', () => {
                const queryKey = 'my-query-key';
                const queryData = [{ id: 1, title: 'Post title', author: { name: 'John Snow' } }];

                return cache.get(queryKey).then(result1 => {
                    assert.isUndefined(result1); // make sure the cache is empty
                    return cache.queries
                        .kset(queryKey, queryData, ['Post', 'Author'])
                        .then(
                            () =>
                                new Promise((resolve, reject) => {
                                    redisClient
                                        .multi([['get', queryKey], ['scard', 'gcq:Post'], ['scard', 'gcq:Author']])
                                        .exec((err, response) => {
                                            if (err) {
                                                return reject(err);
                                            }
                                            expect(JSON.parse(response[0])).deep.equal(queryData);
                                            expect(response[1]).equal(1);
                                            expect(response[2]).equal(1);
                                            return resolve();
                                        });
                                })
                        )
                        .then(() => cache.queries.clearQueriesByKind(['Post', 'Author']))
                        .then(
                            () =>
                                new Promise((resolve, reject) => {
                                    redisClient
                                        .multi([['get', queryKey], ['scard', 'gcq:Post'], ['scard', 'gcq:Author']])
                                        .exec((err, response) => {
                                            if (err) {
                                                return reject(err);
                                            }
                                            expect(response[0]).equal(null);
                                            expect(response[1]).equal(0);
                                            expect(response[2]).equal(0);
                                            return resolve();
                                        });
                                })
                        );
                });
            });
        });
    });
});

describe('Integration Test **wrapped** Datastore', () => {
    let cache;
    let redisClient;

    beforeEach(function integrationTest() {
        if (argv.integration !== true) {
            // Skip e2e tests suite
            this.skip();
        }

        cache = new NsqlCache({
            db: dsAdapter(dsWrapped),
            stores: [{ store: redisStore }],
        });
        ({ redisClient } = cache);
    });

    afterEach(done => {
        redisClient.flushdb(() => done());
    });

    describe('datastore.save()', () => {
        it('should clear the Queries from the entity Kind', () => {
            const userName = string.random();
            const q = ds.createQuery('User').filter('name', userName);
            const entity = Object.assign({}, { name: userName }, { [ds.KEY]: { id: 123 } });
            const queryRes = [entity];

            return cache.queries
                .get(q)
                .then(result1 => {
                    assert.isNull(result1); // make sure the cache is empty
                    return cache.queries.set(q, queryRes).then(
                        () =>
                            // Check that the Set contains our query
                            new Promise((resolve, reject) =>
                                redisClient.scard('gcq:User', (err, total) => {
                                    if (err) {
                                        return reject(err);
                                    }
                                    expect(total).equal(1);
                                    return resolve();
                                })
                            )
                    );
                })
                .then(() => {
                    // Save a new "User"
                    const key = dsWrapped.key(['User']);
                    const data = { name: string.random() };
                    return dsWrapped.save({ key, data }).then(res => {
                        // Make sure the response from the Datastore has been forwarded
                        assert.isDefined(res[0].indexUpdates);
                        return new Promise((resolve, reject) =>
                            redisClient.scard('gcq:User', (err, total) => {
                                if (err) {
                                    return reject(err);
                                }
                                expect(total).equal(0);
                                return resolve();
                            })
                        );
                    });
                });
        });

        it('should work with batch too', () => {
            const userName = string.random();
            const q = ds.createQuery('User').filter('name', userName);
            const q2 = ds.createQuery('User').filter('name', string.random());
            const queryRes = [{ whatever: 123 }];

            return cache.queries
                .get(q)
                .then(result1 => {
                    // make sure the cache is empty
                    assert.isNull(result1);
                    // add the query response to the cache
                    return cache.queries.mset(q, queryRes, q2, queryRes).then(
                        () =>
                            // Check that the Redis Set contains our query
                            new Promise((resolve, reject) => {
                                redisClient.scard('gcq:User', (err, total) => {
                                    if (err) {
                                        return reject(err);
                                    }
                                    expect(total).equal(2);
                                    return resolve();
                                });
                            })
                    );
                })
                .then(() => {
                    // Save a new "User"
                    const key1 = dsWrapped.key(['User', string.random()]);
                    const key2 = dsWrapped.key(['User', string.random()]);
                    const data = { name: string.random() };
                    return dsWrapped.save([{ key: key1, data }, { key: key2, data }]).then(res => {
                        // Make sure the response from the Datastore has been forwarded
                        assert.isDefined(res[0].indexUpdates);

                        return new Promise((resolve, reject) => {
                            redisClient.scard('gcq:User', (err, total) => {
                                if (err) {
                                    return reject(err);
                                }
                                expect(total).equal(0);
                                return resolve();
                            });
                        });
                    });
                });
        });
    });

    describe('datastore.delete()', () => {
        it('should clear the Queries from the entity Kind', () => {
            const userName = string.random();
            const q = ds.createQuery('User').filter('name', userName);
            const entity = Object.assign({}, { name: userName }, { [ds.KEY]: { id: 123 } });
            const queryRes = [entity];

            return cache.queries
                .get(q)
                .then(result1 => {
                    assert.isNull(result1); // make sure the cache is empty
                    return cache.queries.set(q, queryRes).then(
                        () =>
                            // Check that the Set contains our query
                            new Promise((resolve, reject) => {
                                redisClient.scard('gcq:User', (err, total) => {
                                    if (err) {
                                        return reject(err);
                                    }
                                    expect(total).equal(1);
                                    return resolve();
                                });
                            })
                    );
                })
                .then(() => {
                    // Delete user
                    const key = dsWrapped.key(['User', 123]);
                    return dsWrapped.delete(key).then(res => {
                        // Make sure the response from the Datastore has been forwarded
                        assert.isDefined(res[0].indexUpdates);

                        return new Promise((resolve, reject) => {
                            redisClient.scard('gcq:User', (err, total) => {
                                if (err) {
                                    return reject(err);
                                }
                                expect(total).equal(0);
                                return resolve();
                            });
                        });
                    });
                });
        });
    });
});
