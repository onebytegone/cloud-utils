import {
   AttributeValue,
   BatchWriteItemCommand,
   DynamoDBClient,
   WriteRequest,
} from '@aws-sdk/client-dynamodb';
import { marshall } from '@aws-sdk/util-dynamodb';
import { Command, Option } from 'commander';
import PQueue from 'p-queue';
import chalk from 'chalk';
import { hasDefined, isStringUnknownMap } from '@silvermine/toolbox';
import { streamRecordsFromFile } from '../../lib/stream-records-from-file';
import { batchRecords } from '../../lib/batch-records';
import { quitWithError } from '../../lib/quit-with-error';

const BATCH_SIZE = 25,
      STATUS_INTERVAL = BATCH_SIZE * 10;

interface CommandOptions {
   name: string;
   recordsFile: string;
   ddbJsonMarshall: boolean;
   keys?: string;
   concurrency: number;
   region?: string;
}

function pluckKeyFields(record: Record<string, unknown>, keys: string[]): Record<string, unknown> {
   return keys.reduce<Record<string, unknown>>((memo, key) => {
      if (!hasDefined(record, key)) {
         quitWithError(`Key field "${key}" not found in record: ${JSON.stringify(record)}`);
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
         quitWithError(`Field "${field}" is not a valid DynamoDB AttributeValue: ${JSON.stringify(value)}`);
      }

      memo[field] = value;
      return memo;
   }, {});

   return { DeleteRequest: { Key: key } };
}

async function sendBatch(
   client: DynamoDBClient,
   tableName: string,
   requests: WriteRequest[],
   queue: PQueue,
   counters: { deleted: number; failed: number }
): Promise<void> {
   let response;

   try {
      response = await client.send(new BatchWriteItemCommand({
         RequestItems: { [tableName]: requests },
      }));
   } catch(e) {
      counters.failed += requests.length;
      console.error(chalk.red(`Batch failed: ${e instanceof Error ? e.message : String(e)}`));
      return;
   }

   const unprocessed = response.UnprocessedItems?.[tableName];

   if (!unprocessed || unprocessed.length === 0) {
      counters.deleted += requests.length;
   } else {
      counters.deleted += requests.length - unprocessed.length;
      queue.add(async () => { return sendBatch(client, tableName, unprocessed, queue, counters); });
   }

   if ((counters.deleted + counters.failed) % STATUS_INTERVAL === 0) {
      console.info(chalk.gray(`Status: ${counters.deleted} deleted / ${counters.failed} failed`));
   }
}

async function bulkDelete(this: Command, opts: CommandOptions): Promise<void> {
   const client = new DynamoDBClient({ region: opts.region }),
         queue = new PQueue({ concurrency: opts.concurrency }),
         maxQueueSize = opts.concurrency * 5,
         parsedKeys = opts.keys ? opts.keys.split(',').map((k) => { return k.trim(); }) : undefined,
         counters = { deleted: 0, failed: 0 };

   console.info(chalk.yellow(
      `Starting bulk delete from ${opts.recordsFile} on table "${opts.name}" (concurrency: ${opts.concurrency})`
   ));

   for await (const records of batchRecords<Record<string, unknown>>(streamRecordsFromFile(opts.recordsFile), BATCH_SIZE)) {
      const requests = records.map((record): WriteRequest => {
         return buildWriteRequest(record, parsedKeys, opts.ddbJsonMarshall);
      });

      queue.add(async () => { return sendBatch(client, opts.name, requests, queue, counters); });

      if (queue.size > maxQueueSize) {
         await queue.onEmpty();
      }
   }

   await queue.onIdle();

   console.info(chalk.whiteBright(
      `Total: ${counters.deleted + counters.failed} records (${counters.deleted} deleted / ${counters.failed} failed)`
   ));
}

export default function register(command: Command): void {
   /* eslint-disable @silvermine/silvermine/call-indentation */
   command
      .description('Deletes the provided records from the specified DynamoDB table')
      .requiredOption('--name <string>', 'name of the table')
      .requiredOption('-i, --records-file <path>', 'path to the input file (.ndjson, .jsonl, .csv, or .tsv)')
      .option('--keys <fields>', 'comma-separated field names to use as the DynamoDB key (e.g. "pk,sk")')
      .option('--no-ddb-json-marshall', 'don\'t attempt to marshall the records into DynamoDB JSON')
      .option('--region <value>', 'region to send requests to')
      .addOption(
         new Option('--concurrency <number>', 'number of concurrent BatchWriteItem requests')
            .argParser((value) => { return Number(value); })
            .default(10)
      )
      .action(bulkDelete);
   /* eslint-enable @silvermine/silvermine/call-indentation */
}
