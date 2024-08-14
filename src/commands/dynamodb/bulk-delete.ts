import { Command, Option } from 'commander';
import PQueue from 'p-queue';
import { streamLinesFromFile } from '../../../lib/stream-lines-from-file';
import { batchRecords } from '../../../lib/batch-records';
import { BatchWriteItemCommand, DynamoDB, WriteRequest } from '@aws-sdk/client-dynamodb';
import { marshall } from '@aws-sdk/util-dynamodb';

interface CommandOptions {
   name: string;
   recordsFile: string;
   ddbJsonMarshall: boolean;
   concurrency: number;
}

const dyndb = new DynamoDB({});

async function bulkDelete(this: Command, opts: CommandOptions): Promise<void> {
   const queue = new PQueue({ concurrency: opts.concurrency }),
         maxQueueSize = opts.concurrency * 5;

   for await (const records of batchRecords(streamLinesFromFile(opts.recordsFile), 25)) {
      queue.add(async () => {
         await dyndb.send(new BatchWriteItemCommand({
            RequestItems: {
               [opts.name]: records.map((record): WriteRequest => {
                  const parsedRecord = JSON.parse(record);

                  return {
                     DeleteRequest: {
                        Key: opts.ddbJsonMarshall ? marshall(parsedRecord) : parsedRecord,
                     },
                  };
               }),
            },
         }));
      });

      if (queue.size > maxQueueSize) {
         await queue.onEmpty();
      }
   }

   await queue.onIdle();
}

export default function register(command: Command): void {
   /* eslint-disable @silvermine/silvermine/call-indentation */
   command
      .description(
         'Deletes the provided records from the specified DynamoDB table'
      )
      .requiredOption('--name <string>', 'name of the table')
      .requiredOption('-i, --records-file <path>', 'name of the file containing newline-delimited records')
      .option('--failed <path>', 'name of the file to write the responses from failed invocations')
      .option('--successful <path>', 'name of the file to write the response from successful invocations')
      .option('--no-ddb-json-marshall', 'Don\'t attempt to marshall the records into DynamoDB JSON')
      .addOption(
         new Option('--concurrency <number>', 'number of concurrent invocation requests')
            .argParser((value) => {
               return Number(value);
            })
            .default(10)
      )
      .action(bulkDelete);
   /* eslint-enable @silvermine/silvermine/call-indentation */
}
