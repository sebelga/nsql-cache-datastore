'use strict';

const chai = require('chai');
const ds = require('@google-cloud/datastore')();
const dsAdapter = require('../lib')(ds);

const { keyToString, queryToString } = dsAdapter;
const { expect } = chai;

const separator = ':%:';

describe('Google Datastore adapter', () => {
    describe('keyToString', () => {
        const key1 = ds.key({ namespace: 'ns', path: ['User', 111] });
        const key2 = ds.key(['GranDad', 'John', 'Dad', 'Mick', 'User', 555]);

        it('should convert the query to string', () => {
            const str1 = keyToString(key1, { hash: false });
            const str2 = keyToString(key2, { hash: false });

            expect(str1).equal(`ns${separator}User${separator}111`);
            expect(str2).equal(`GranDad${separator}John${separator}Dad${separator}Mick${separator}User${separator}555`);
        });

        it('should throw an error if no Key passed', () => {
            const fn = () => keyToString();
            expect(fn).throws('Key cannot be undefined.');
        });
    });

    describe('dsQueryToString', () => {
        const q1 = ds
            .createQuery('com.domain.dev', 'Company')
            .filter('name', 'Sympresa')
            .filter('field1', '<', 123)
            .filter('field2', '>', 789)
            .groupBy(['field1', 'field2'])
            .hasAncestor(ds.key(['Parent', 123]))
            .limit(10)
            .offset(5)
            .order('size', { descending: true })
            .select(['name', 'size'])
            .start('X')
            .end('Y');

        const q2 = ds
            .createQuery('User')
            .filter('name', 'john')
            .order('phone');

        const q3 = ds
            .createQuery('Task')
            .select('__key__')
            .filter('__key__', '>', ds.key(['Task', 'someTask']));

        it('should convert the query to string', () => {
            const str1 = queryToString(q1, { hash: false });
            const str2 = queryToString(q2, { hash: false });
            const str3 = queryToString(q3, { hash: false });

            expect(str1).equal(
                `Company${separator}com.domain.dev${separator}name=Sympresafield1<123field2>789__key__HAS_ANCESTORParent${separator}123${separator}field1field2${separator}10${separator}5${separator}size-${separator}namesize${separator}X${separator}Y` // eslint-disable-line
            );
            expect(str2).equal(
                `User${separator + separator}name=john${separator +
                    separator}-1${separator}-1${separator}phone+${separator + separator + separator}`
            );
            expect(str3).equal(
                `Task${separator + separator}__key__>Task${separator}someTask${separator +
                    separator}-1${separator}-1${separator + separator}__key__${separator + separator}`
            );
        });
    });
});
