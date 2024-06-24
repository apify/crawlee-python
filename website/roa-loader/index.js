const { inspect } = require('util');

const { urlToRequest } = require('loader-utils');

const signingUrl = new URL('https://api.apify.com/v2/tools/encode-and-sign');
signingUrl.searchParams.set('token', process.env.APIFY_SIGNING_TOKEN);
const queue = [];
let working = false;

async function getHash(source) {
    const memory = source.match(/playwright|puppeteer/i) ? 4096 : 1024;
    const res = await (await fetch(signingUrl, {
        method: 'POST',
        body: JSON.stringify({
            input: JSON.stringify({ code: source }),
            options: {
                build: 'latest',
                contentType: 'application/json; charset=utf-8',
                memory,
                timeout: 180,
            },
        }),
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
        },
    }));

    if (!res.ok) {
        console.error(`Signing failed: ${res.status} ${res.statusText}`, await res.text());
        return 'invalid-token';
    }

    const body = await res.json();

    await new Promise((resolve) => setTimeout(resolve, 100));

    if (!body.data || !body.data.encoded) {
        console.error(`Signing failed:' ${inspect(body.error) || 'Unknown error'}`, body);
        return 'invalid-token';
    }

    return body.data.encoded;
}

async function encodeAndSign(source) {
    if (!process.env.APIFY_SIGNING_TOKEN) {
        return 'invalid-token';
    }

    if (working) {
        return new Promise((resolve, reject) => {
            queue.push(() => {
                return getHash(source).then(resolve, reject);
            });
        });
    }

    let res;

    try {
        working = true;
        res = await getHash(source);

        while (queue.length) {
            await queue.shift()();
        }
    } finally {
        working = false;
    }

    return res;
}

module.exports = async function (code) {
    // TODO enable once we have python example runner actor
    return { code };

    if (process.env.CRAWLEE_DOCS_FAST) {
        return { code, hash: 'fast' };
    }

    console.log(`Signing ${urlToRequest(this.resourcePath)}...`, { working, queue: queue.length });
    const hash = await encodeAndSign(code);
    return { code, hash };
};
