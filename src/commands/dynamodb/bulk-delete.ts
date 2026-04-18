import { WriteStream } from 'fs';
import {
   AttributeValue,
   BatchWriteItemCommand,
   DynamoDBClient,
   WriteRequest,
} from '@aws-sdk/client-dynamodb';
import { marshall } from '@aws-sdk/util-dynamodb';
import PQueue from 'p-queue';
import chalk from 'chalk';
import { Flags } from '@oclif/core';
import { hasDefined, isStringUnknownMap } from '@silvermine/toolbox';
import { streamRecordsFromFile } from '../../lib/stream-records-from-file.js';
import { batchRecords } from '../../lib/batch-records.js';
import { generateDefaultOutputFilename } from '../../lib/generate-default-output-filename.js';
import createWriteStream from '../../lib/create-write-stream.js';
import { BaseCommand } from '../../base-command.js';

const BATCH_SIZE = 25,
      STATUS_INTERVAL = BATCH_SIZE * 10;

function pluckKeyFields(record: Record<string, unknown>, keys: string[]): Record<string, unknown> {
   return keys.reduce<Record<string, unknown>>((memo, key) => {
      if (!hasDefined(record, key)) {
         throw new Error(`Key field "${key}" not found in record: ${JSON.stringify(record)}`);
      }

      memo[key] = record[key];
      return memo;
   }, {});
}

function isAttributeValue(value: unknown): value is AttributeValue {
   return isStringUnknownMap(value);
}

function buildWriteRequest(record: Record<string, unknown>, keys: string[] | undefined, shouldMarshall: boolean): WriteRequest {
   const keyRecord = keys ? pluckKeyFields(record, keys) : record;

   if (shouldMarshall) {
      return { DeleteRequest: { Key: marshall(keyRecord) } };
   }

   const key = Object.entries(keyRecord).reduce<Record<string, AttributeValue>>((memo, [ field, value ]) => {
      if (!isAttributeValue(value)) {
         throw new Error(`Field "${field}" is not a valid DynamoDB AttributeValue: ${JSON.stringify(value)}`);
      }

      memo[field] = value;
      return memo;
   }, {});

   return { DeleteRequest: { Key: key } };
}

interface SendBatchOptions {
   queue: PQueue;
   counters: { deleted: number; failed: number };
   failedWriteStream: WriteStream;
   log: (msg: string) => void;
   warn: (msg: string) => void;
}

function recordBatchFailure(requests: WriteRequest[], error: unknown, opts: SendBatchOptions): void {
   const message = error instanceof Error ? error.message : String(error);

   opts.counters.failed += requests.length;
   opts.warn(chalk.red(`Batch failed (${requests.length} records): ${message}`));

   for (const request of requests) {
      opts.failedWriteStream.write(JSON.stringify({ request, error: message }) + '\n');
   }
}

async function sendBatch(
   client: DynamoDBClient,
   tableName: string,
   requests: WriteRequest[],
   opts: SendBatchOptions
): Promise<void> {
   let response;

   try {
      response = await client.send(new BatchWriteItemCommand({
         RequestItems: { [tableName]: requests },
      }));
   } catch(e) {
      recordBatchFailure(requests, e, opts);
      return;
   }

   const unprocessed = response.UnprocessedItems?.[tableName];

   if (!unprocessed || unprocessed.length === 0) {
      opts.counters.deleted += requests.length;
   } else {
      opts.counters.deleted += requests.length - unprocessed.length;
      opts.queue.add(async () => {
         try {
            await sendBatch(client, tableName, unprocessed, opts);
         } catch(e) {
            recordBatchFailure(unprocessed, e, opts);
         }
      });
   }

   if ((opts.counters.deleted + opts.counters.failed) % STATUS_INTERVAL === 0) {
      opts.log(chalk.gray(`Status: ${opts.counters.deleted} deleted / ${opts.counters.failed} failed`));
   }
}

export default class BulkDelete extends BaseCommand {

   public static summary = 'Delete records from a DynamoDB table';

   public static flags = {
      name: Flags.string({
         description: 'name of the table',
         required: true,
      }),
      'records-file': Flags.string({
         char: 'i',
         description: 'path to the input file (.ndjson, .jsonl, .csv, or .tsv)',
         required: true,
      }),
      failed: Flags.string({
         description: 'name of the file to write information about failed deletions',
      }),
      keys: Flags.string({
         description: 'comma-separated field names to use as the DynamoDB key (e.g. "pk,sk")',
      }),
      'ddb-json-marshall': Flags.boolean({
         description: 'marshall the records into DynamoDB JSON',
         default: true,
         allowNo: true,
      }),
      concurrency: Flags.integer({
         description: 'number of concurrent BatchWriteItem requests',
         default: 10,
      }),
   };

   public async run(): Promise<void> {
      const { flags } = await this.parse(BulkDelete),
            client = new DynamoDBClient({ region: flags.region }),
            queue = new PQueue({ concurrency: flags.concurrency }),
            maxQueueSize = flags.concurrency * 5,
            parsedKeys = flags.keys ? flags.keys.split(',').map((k) => { return k.trim(); }) : undefined,
            failedOutputFile = flags.failed || generateDefaultOutputFilename('dynamodb', 'bulk-delete', flags.name, 'failed');

      const failedWriteStream = await createWriteStream(failedOutputFile),
            counters = { deleted: 0, failed: 0 };

      const batchOpts: SendBatchOptions = {
         queue,
         counters,
         failedWriteStream,
         log: this.log.bind(this),
         warn: this.logToStderr.bind(this),
      };

      this.log(chalk.yellow(
         `Starting bulk delete from ${flags['records-file']} on table "${flags.name}" (concurrency: ${flags.concurrency})`
      ));
      this.log(`${chalk.gray('Failed output:')} ${failedOutputFile}`);

      for await (const records of batchRecords<Record<string, unknown>>(streamRecordsFromFile(flags['records-file']), BATCH_SIZE)) {
         const requests = records.map((record): WriteRequest => {
            return buildWriteRequest(record, parsedKeys, flags['ddb-json-marshall']);
         });

         queue.add(async () => {
            try {
               await sendBatch(client, flags.name, requests, batchOpts);
            } catch(e) {
               recordBatchFailure(requests, e, batchOpts);
            }
         });

         if (queue.size > maxQueueSize) {
            await queue.onEmpty();
         }
      }

      await queue.onIdle();

      failedWriteStream.end();

      this.log(chalk.whiteBright(
         `Total: ${counters.deleted + counters.failed} records (${counters.deleted} deleted / ${counters.failed} failed)`
      ));

      if (counters.failed > 0) {
         this.exit(1);
      }
   }

}
