import { createReadStream, createWriteStream } from 'fs';
import { resolve } from 'path';
import { parse, format } from 'fast-csv';
import { once } from 'events';
import { finished } from 'stream';

const dataDir = resolve(__dirname, '../data');

const inputFiles = [
    `${dataDir}/1.csv`,
    `${dataDir}/2.csv`,
    `${dataDir}/3.csv`
];

const idHeader = 'id';

const ouputFile = `${dataDir}/out.csv`;

const inputFileOptions = { encoding: 'utf8' };

const inputCsvOptions = { headers: true };

const outputFileOptions = { encoding: 'utf8' };

const outputCsvOptions = {};

async function* createCsvIterator<T extends Record<string, any>>(filename: string): AsyncGenerator<T, undefined, T> {
    const fileStream = createReadStream(filename, inputFileOptions);
    const csvStream = parse(inputCsvOptions);
    
    fileStream.pipe(csvStream);

    for await (const row of csvStream) {
        yield {
            ...row,
            [idHeader]: parseInt(row[idHeader], 10)
        };
    }

    return;
}

function* reverseIterator<T = any>(arr: Array<T>): Iterable<[number, T]> {
    for (let key = arr.length - 1; key > -1; key--) {
        yield [key, arr[key]];
    }
}

async function run() {
    //configure inputs - reverse here as we mutate the inputs later reading in reverse
    const inputs = inputFiles.map((filename) => createCsvIterator(filename));
    inputs.reverse();

    const allHeaders = new Set<string>();
    const objs = await Promise.all(inputs.map(input => input.next()));
    
    for (const [key, obj] of reverseIterator(objs)) {
        if (obj.done) {
            inputs.splice(key, 1);
            objs.splice(key, 1);
        } else {
            Object.keys(obj.value).forEach((header) => allHeaders.add(header));
        }
    }

    //configure output, using all headers gathered from the inputs as default
    const outFileStream = createWriteStream(ouputFile, outputFileOptions);
    const outCsvStream = format({ headers: Array.from(allHeaders), ...outputCsvOptions });

    outCsvStream.pipe(outFileStream);

    //seesaw down our collections and merge objects with the same id, keeping the order intact
    while (objs.length) {
        const minId = Math.min(...objs.map((obj) => obj.value![idHeader]));

        const newObj = {};

        for (const [key, obj] of reverseIterator(objs)) {
            if (obj.value![idHeader] === minId) {
                Object.assign(newObj, obj.value);

                const nextObj = await inputs[key].next();

                if (nextObj.done) {
                    inputs.splice(key, 1);
                    objs.splice(key, 1);
                } else {
                    objs[key] = nextObj;
                }
            }
        }
        
        if (!outCsvStream.write(newObj)) {
            await once(outFileStream, 'drain');
        }
    }

    outFileStream.end();

    await new Promise((res, rej) => finished(outFileStream, (err) => err ? rej(err) : res));
}

run();
