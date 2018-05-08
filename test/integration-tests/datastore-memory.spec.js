'use strict';

const { argv } = require('yargs');

const Datastore = require('@google-cloud/datastore');
const chai = require('chai');
const sinon = require('sinon');
const NsqlCache = require('nsql-cache');

const dsAdapter = require('../../lib');
const { string } = require('../utils');

const ds = new Datastore({ projectId: 'gstore-cache-integration-tests' });
const dsWrapped = new Datastore({ projectId: 'gstore-cache-integration-tests' });

const { expect, assert } = chai;

// Helper to add some timeout between saving the entity and retrieving it
const addTimeOut = () => new Promise(resolve => setTimeout(resolve, 1000));

describe('Integration Tests (Datastore & Memory cache)', () => {
    let cache;

    beforeEach(function integrationTest() {
        if (argv.integration !== true) {
            // Skip e2e tests suite
            this.skip();
        }

        cache = new NsqlCache({ db: dsAdapter(ds), config: { wrapClient: false } });
    });

    it('check that Local Datastore is up and running', () => {
        const key = ds.key(['User', string.random()]);
        const entityData = { name: string.random() };

        return ds.get(key).then(res => {
            expect(typeof res[0]).equal('undefined');

            return ds
                .save({ key, data: entityData })
                .then(() => ds.get(key))
                .then(res2 => {
                    expect(res2[0]).deep.equal(entityData);
                });
        });
    });

    describe('gstoreCache.keys', () => {
        beforeEach(function TestGstoreCacheKeys() {
            if (argv.integration !== true) {
                // Skip e2e tests suite
                this.skip();
            }
            sinon.spy(ds, 'get');
        });

        afterEach(() => {
            ds.get.restore();
        });

        describe('set()', () => {
            it('should add data to cache', () => {
                const key = ds.key(['User', string.random()]);
                const entityData = { age: 20 };
                return ds.save({ key, data: entityData }).then(() =>
                    cache.keys.get(key).then(result1 => {
                        assert.isUndefined(result1); // make sure the cache is empty
                        return ds
                            .get(key)
                            .then(result2 => cache.keys.set(key, result2[0]))
                            .then(() => cache.keys.get(key))
                            .then(result3 => {
                                expect(result3).deep.equal(entityData);
                            });
                    })
                );
            });

            it('should set multiple keys in cache and return saved values', () => {
                const key1 = ds.key(['User', string.random()]);
                const key2 = ds.key(['User', string.random()]);
                const entityData1 = { name: string.random() };
                const entityData2 = { name: string.random() };
                return cache.keys.mset(key1, entityData1, key2, entityData2).then(result => {
                    expect(result[0]).equal(entityData1);
                    expect(result[1]).equal(entityData2);
                });
            });

            it('should set the key in the cache', () => {
                const key = ds.key(['User', string.random()]);
                const entityData = { age: 20 };
                return cache.keys
                    .get(key)
                    .then(result => {
                        assert.isUndefined(result);
                    })
                    .then(() => cache.keys.set(key, entityData))
                    .then(() => cache.keys.read(key))
                    .then(result => {
                        expect(result).contains(entityData);
                        expect(ds.get.called).equal(false);
                    })
                    .then(() => cache.keys.get(key))
                    .then(result => {
                        expect(result).contains(entityData);
                        expect(result[ds.KEY]).equal(key);
                    });
            });
        });

        describe('read()', () => {
            it('should add data to cache', () => {
                const key = ds.key(['User', string.random()]);
                const entityData = { age: 20 };

                return ds.save({ key, data: entityData }).then(() =>
                    cache.keys
                        .read(key)
                        .then(result => {
                            expect(result).deep.equal(entityData);
                        })
                        .then(() =>
                            cache.keys.read(key).then(result => {
                                expect(result).deep.equal(entityData);
                                expect(result[ds.KEY]).equal(key);
                                expect(ds.get.callCount).equal(1);
                            })
                        )
                );
            });

            it('should allow multiple keys', () => {
                const key1 = ds.key(['User', string.random()]);
                const key2 = ds.key(['User', string.random()]);
                const entityData1 = { name: string.random() };
                const entityData2 = { name: string.random() };

                return ds.save([{ key: key1, data: entityData1 }, { key: key2, data: entityData2 }]).then(() =>
                    cache.keys
                        .read([key1, key2])
                        .then(result => {
                            expect(result[0]).deep.equal(entityData1);
                            expect(result[1]).deep.equal(entityData2);
                        })
                        .then(() =>
                            cache.keys.read([key1, key2]).then(result => {
                                expect(result[0]).deep.equal(entityData1);
                                expect(result[1]).deep.equal(entityData2);
                                expect(result[0][ds.KEY]).equal(key1);
                                expect(result[1][ds.KEY]).equal(key2);
                                expect(ds.get.callCount).equal(1);
                            })
                        )
                );
            });

            it('should return undefined when Key not found', () =>
                cache.keys.read(ds.key(['User', string.random()])).then(result => {
                    assert.isUndefined(result);
                }));
        });
    });

    describe('gstoreCache.queries', () => {
        beforeEach(function TestGstoreCacheQueries() {
            if (argv.integration !== true) {
                // Skip e2e tests suite
                this.skip();
            }
        });

        describe('set()', () => {
            it('should add query data to cache', () => {
                const queryRes = [{ name: string.random() }];
                const q = ds.createQuery(string.random()).filter('age', 20);
                return cache.queries.get(q).then(result1 => {
                    assert.isUndefined(result1); // make sure the cache is empty

                    return cache.queries
                        .set(q, queryRes)
                        .then(() => cache.queries.get(q))
                        .then(cacheData => {
                            const [entities] = cacheData;
                            expect(entities).deep.equal(queryRes);
                        });
                });
            });
        });

        describe('mget() & mset()', () => {
            it('should set and return multiple queries', () => {
                const q1 = ds.createQuery(string.random()).filter('age', 20);
                const q2 = ds.createQuery(string.random()).filter('age', 20);
                const queryRes1 = [{ name: string.random() }];
                const queryRes2 = [{ name: string.random() }];

                return cache.queries.mget(q1, q2).then(result1 => {
                    assert.isUndefined(result1[0]);
                    assert.isUndefined(result1[1]);
                    return cache.queries
                        .mset(q1, queryRes1, q2, queryRes2)
                        .then(() => cache.queries.mget(q1, q2))
                        .then(result2 => {
                            const [res1] = result2[0];
                            const [res2] = result2[1];
                            expect(res1).deep.equal(queryRes1);
                            expect(res2).deep.equal(queryRes2);
                        });
                });
            });
        });
    });
});

// ------------------------------------------------------------------
// ----- WRAPPED DATASTORE CLIENT
// ------------------------------------------------------------------

describe('Integration Test **wrapped** Datastore', () => {
    let cache;

    beforeEach(function integrationTest() {
        if (argv.integration !== true) {
            // Skip e2e tests suite
            this.skip();
        }

        cache = new NsqlCache({ db: dsAdapter(dsWrapped) });
    });

    describe('datastore.get()', () => {
        it('should get the data from Datastore and prime the cache', () => {
            const key = ds.key(['User', string.random()]);
            const entityData = { name: string.random() };

            return cache.keys
                .read(key)
                .then(result => {
                    assert.isUndefined(result);
                })
                .then(() =>
                    dsWrapped.save({ key, data: entityData }).then(() =>
                        dsWrapped
                            .get(key)
                            .then(result => {
                                expect(result).deep.equal([entityData]);
                            })
                            .then(() =>
                                cache.keys.read(key).then(result => {
                                    expect(result).contains(entityData);
                                    expect(result[dsWrapped.KEY]).deep.equal(key);
                                })
                            )
                    )
                );
        });

        it('should bypass the cache', () => {
            const oldName = string.random();
            const newName = string.random();

            const key = ds.key(['User', string.random()]);
            const entityData = { name: newName };

            return cache.keys.set(key, { name: oldName }).then(() =>
                dsWrapped.save({ key, data: entityData }, { cache: false }).then(() =>
                    dsWrapped.get(key, { cache: false }).then(result => {
                        expect(result[0].name).equal(newName);
                    })
                )
            );
        });

        it('should bypass the cache when global configuration is set to false', () => {
            cache.config.global = false;

            const oldName = string.random();
            const newName = string.random();

            const key = ds.key(['User', string.random()]);
            const entityData = { name: newName };

            return cache.keys.set(key, { name: oldName }).then(() =>
                dsWrapped.save({ key, data: entityData }, { cache: false }).then(() =>
                    dsWrapped.get(key).then(result => {
                        expect(result[0].name).equal(newName);
                    })
                )
            );
        });

        it('should get from the cache when forced in inline options', () => {
            cache.config.global = false;

            const oldName = string.random();
            const newName = string.random();
            const key = ds.key(['User', string.random()]);

            return dsWrapped.save({ key, data: { name: oldName } }, { cache: true }).then(() =>
                cache.keys.set(key, { name: newName }).then(() =>
                    dsWrapped.get(key, { cache: true }).then(result => {
                        expect(result[0].name).equal(newName);
                    })
                )
            );
        });

        it('should allow custom ttl value', () => {
            sinon.spy(cache, 'primeCache');

            const key = ds.key(['User', string.random()]);

            return cache.keys
                .get(key)
                .then(result => {
                    assert.isUndefined(result);
                })
                .then(() =>
                    dsWrapped.save({ key, data: { a: 123 } }, { cache: false }).then(() =>
                        dsWrapped.get(key, { cache: { ttl: 778 } }).then(() => {
                            const { args } = cache.primeCache.getCall(0);
                            expect(args[2].ttl).equal(778);
                            cache.primeCache.restore();
                        })
                    )
                );
        });
    });

    describe('datastore.createQuery()', () => {
        it('should add query data to cache', () => {
            const userName = string.random();

            const q = dsWrapped
                .createQuery('User')
                .filter('name', userName)
                .hasAncestor(ds.key(['Parent', 'default']));

            return cache.queries.get(q).then(result1 => {
                assert.isUndefined(result1); // make sure the cache is empty

                const key = dsWrapped.key(['Parent', 'default', 'User', 123]);
                const data = { name: userName };

                return dsWrapped
                    .save({ key, data })
                    .then(addTimeOut)
                    .then(() =>
                        q
                            .run()
                            .then(() => cache.queries.get(q)) // check the content of the cache for this Query
                            .then(result2 => {
                                const [entities] = result2;
                                expect(entities).deep.equal([data]);
                            })
                    );
            });
        });

        it('should bypass the cache', () => {
            const team = string.random();
            const q = dsWrapped
                .createQuery('User')
                .filter('age', '>', 20)
                .filter('team', team);

            return cache.queries.get(q).then(result1 => {
                assert.isUndefined(result1); // make sure the cache is empty

                const key1 = dsWrapped.key(['User']);
                const key2 = dsWrapped.key(['User']);
                const data1 = { age: 21, team };
                const data2 = { age: 22, team };

                return dsWrapped
                    .save({ key: key1, data: data1 })
                    .then(addTimeOut)
                    .then(() =>
                        q.run().then(result2 => {
                            const [entities] = result2;
                            expect(entities.length).equal(1);
                        })
                    )
                    .then(() => dsWrapped.save({ key: key2, data: data2 }, { cache: false }))
                    .then(addTimeOut)
                    .then(() =>
                        q.run({ cache: false }).then(result2 => {
                            const [entities] = result2;
                            expect(entities.length).equal(2);
                        })
                    );
            });
        });

        it('should by pass the cache when global configuration is set to false', () => {
            cache.config.global = false;
            const category = string.random();
            const q = dsWrapped.createQuery('User').filter('category', category);
            const key1 = dsWrapped.key(['User']);
            const key2 = dsWrapped.key(['User']);

            return dsWrapped
                .save([{ key: key1, data: { category } }, { key: key2, data: { category } }])
                .then(
                    () => cache.queries.set(q, [{ category }]) // prime the cache with only 1 result
                )
                .then(() => q.run())
                .then(([entities]) => {
                    expect(entities.length).equal(2); // make sure we we do have our 2 entities
                });
        });

        it('should allow custom ttl value', () => {
            sinon.spy(cache, 'primeCache');
            const q = dsWrapped.createQuery('User');

            return q.run({ cache: { ttl: 888 } }).then(() => {
                const { args } = cache.primeCache.getCall(0);
                expect(args[2].ttl).equal(888);
                cache.primeCache.restore();
            });
        });
    });

    describe('datastore.save()', () => {
        it('should add the entity saved to the cache', () => {
            const key = dsWrapped.key(['User', string.random()]);
            const data = { name: string.random() };
            return cache.keys.get(key).then(result1 => {
                assert.isUndefined(result1);

                return dsWrapped
                    .save({ key, data })
                    .then(() => cache.keys.get(key))
                    .then(result2 => {
                        expect(result2).contains(data);
                    });
            });
        });

        it('should work with batch save', () => {
            const key1 = dsWrapped.key(['User', string.random()]);
            const data1 = { name: string.random() };
            const key2 = dsWrapped.key(['User', string.random()]);
            const data2 = { name: string.random() };
            return cache.keys.mget(key1, key2).then(result1 => {
                assert.isUndefined(result1[0]);
                assert.isUndefined(result1[1]);

                return dsWrapped
                    .save([{ key: key1, data: data1 }, { key: key2, data: data2 }])
                    .then(() => cache.keys.mget(key1, key2))
                    .then(result2 => {
                        expect(result2[0]).contains(data1);
                        expect(result2[1]).contains(data2);
                    });
            });
        });

        it('should not prime the cache', () => {
            const key = ds.key(['User', string.random()]);

            return cache.keys
                .get(key)
                .then(result => {
                    assert.isUndefined(result);
                })
                .then(() =>
                    dsWrapped.save({ key, data: { a: 123 } }, { cache: false }).then(() =>
                        cache.keys.get(key).then(result => {
                            assert.isUndefined(result);
                        })
                    )
                );
        });

        it('should allow custom ttl value', () => {
            sinon.spy(cache, 'set');

            const key = ds.key(['User', string.random()]);

            return cache.keys
                .get(key)
                .then(result => {
                    assert.isUndefined(result);
                })
                .then(() =>
                    dsWrapped.save({ key, data: { a: 123 } }, { cache: { ttl: 777 } }).then(() => {
                        const { args } = cache.set.getCall(0);
                        expect(args[2].ttl).equal(777);
                        cache.set.restore();
                    })
                );
        });
    });

    describe('datastore.update()', () => {
        it('should update the cache', () => {
            const oldName = string.random();
            const newName = string.random();
            const key = dsWrapped.key(['User', string.random()]);
            const data = { name: oldName };
            return cache.keys.get(key).then(result1 => {
                assert.isUndefined(result1);

                return dsWrapped
                    .save({ key, data })
                    .then(() => cache.keys.get(key))
                    .then(cacheData => {
                        expect(cacheData.name).equal(oldName);
                    })
                    .then(() => dsWrapped.update({ key, data: { name: newName } }))
                    .then(() => cache.keys.get(key))
                    .then(cacheData => {
                        expect(cacheData.name).equal(newName);
                    });
            });
        });

        it('should return error if key to update not in Datastore', done => {
            const key = dsWrapped.key(['User', string.random()]);
            cache.keys.get(key).then(result1 => {
                assert.isUndefined(result1);

                return dsWrapped.update({ key, data: { name: 'abc' } }).catch(e => {
                    expect(e.code).equal(5);
                    done();
                });
            });
        });
    });

    describe('datastore.insert()', () => {
        it('should update the cache', () => {
            const name = string.random();
            const key = dsWrapped.key(['User', string.random()]);
            const data = { name };

            return cache.keys.get(key).then(result1 => {
                assert.isUndefined(result1);

                return dsWrapped
                    .insert({ key, data })
                    .then(() => cache.keys.get(key))
                    .then(cacheData => {
                        expect(cacheData.name).equal(name);
                    });
            });
        });

        it('should return error if key to insert exists in Datastore', done => {
            const key = dsWrapped.key(['User', string.random()]);

            cache.keys.get(key).then(result1 => {
                assert.isUndefined(result1);

                dsWrapped.save({ key, data: { a: 123 } }).then(() =>
                    dsWrapped.insert({ key, data: { a: 456 } }).catch(e => {
                        expect(e.code).equal(6);
                        done();
                    })
                );
            });
        });
    });

    describe('datastore.upsert()', () => {
        it('should update the cache', () => {
            const name = string.random();
            const key = dsWrapped.key(['User', string.random()]);
            const data = { name };

            return cache.keys.get(key).then(result1 => {
                assert.isUndefined(result1);

                return dsWrapped
                    .upsert({ key, data })
                    .then(() => cache.keys.get(key))
                    .then(cacheData => {
                        expect(cacheData.name).equal(name);
                    });
            });
        });
    });

    describe('datastore.delete()', () => {
        it('should remove the entity from the cache', () => {
            const key = dsWrapped.key(['User', string.random()]);
            const data = { name: string.random() };
            return cache.keys.get(key).then(result1 => {
                assert.isUndefined(result1);

                return dsWrapped
                    .save({ key, data })
                    .then(() => dsWrapped.delete(key))
                    .then(() => cache.keys.get(key))
                    .then(result => {
                        assert.isUndefined(result);
                    });
            });
        });

        it('should work with batch delete', () => {
            const key1 = dsWrapped.key(['User', string.random()]);
            const data1 = { name: string.random() };
            const key2 = dsWrapped.key(['User', string.random()]);
            const data2 = { name: string.random() };
            return cache.keys.mget(key1, key2).then(result1 => {
                assert.isUndefined(result1[0]);
                assert.isUndefined(result1[1]);

                return dsWrapped
                    .save([{ key: key1, data: data1 }, { key: key2, data: data2 }])
                    .then(() => dsWrapped.delete([key1, key2]))
                    .then(() => cache.keys.mget(key1, key2))
                    .then(result2 => {
                        assert.isUndefined(result2[0]);
                        assert.isUndefined(result2[1]);
                    });
            });
        });
    });
});
