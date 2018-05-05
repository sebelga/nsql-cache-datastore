'use strict';

const randomString = (length = 8) => {
    const chars = 'abcdefghiklmnopqrstuvwxyz';
    let randomStr = '';

    for (let i = 0; i < length; i += 1) {
        const rnum = Math.floor(Math.random() * chars.length);
        randomStr += chars.substring(rnum, rnum + 1);
    }

    return randomStr;
};

module.exports = {
    string: {
        random: randomString,
    },
};
