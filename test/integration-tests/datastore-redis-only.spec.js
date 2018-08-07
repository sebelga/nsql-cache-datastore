'use strict';

const Datastore = require('@google-cloud/datastore');
const chai = require('chai');
const redisStore = require('cache-manager-redis-store');
const NsqlCache = require('nsql-cache');
const { argv } = require('yargs');

const dsAdapter = require('../../lib');
const { string } = require('../utils');

const ds = new Datastore({ projectId: 'gstore-cache-integration-tests' });

const { expect } = chai;

describe('Integration Tests (Datastore & Redis cache)', () => {
    let cache;
    let redisClient;

    beforeEach(function integrationTest() {
        if (argv.integration !== true) {
            // Skip e2e tests suite
            this.skip();
        }

        cache = new NsqlCache({
            stores: [{ store: redisStore }],
            db: dsAdapter(ds),
            config: {
                wrapClient: false,
            },
        });

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

        it('should add the entity to the cache and return the entity (single key)', () => {
            const key1 = ds.key(['User', string.random()]);
            const name = string.random();
            const entityData1 = { name };

            return cache.keys.set(key1, entityData1, { ttl: 6000 }).then(result => {
                expect(result).deep.equal(entityData1);
            });
        });

        it('should add the entity to the cache and return the entity (multi keys)', () => {
            const key1 = ds.key(['User', string.random()]);
            const key2 = ds.key(['User', string.random()]);
            const name1 = string.random();
            const name2 = string.random();
            const entityData1 = { name: name1 };
            const entityData2 = { name: name2 };

            return cache.keys.mset(key1, entityData1, key2, entityData2, { ttl: 6000 }).then(result => {
                expect(result[0]).deep.equal(entityData1);
                expect(result[1]).deep.equal(entityData2);
            });
        });

        it('read() should add query data to Redis Cache and return the entity fetched', () => {
            const userName = string.random();
            const entityName = string.random();
            const key = ds.key(['User', entityName]);
            const entityData = { name: userName };

            return ds.save({ key, data: entityData }).then(() =>
                cache.keys.read(key).then(([result1]) => {
                    expect(result1).deep.equal(entityData);
                })
            );
        });
    });
});
