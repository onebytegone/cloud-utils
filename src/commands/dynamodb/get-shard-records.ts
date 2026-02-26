import {
   DynamoDBStreamsClient,
   GetRecordsCommand,
   GetShardIteratorCommand,
   ShardIteratorType,
} from '@aws-sdk/client-dynamodb-streams';
import { Command } from 'commander';
import { isString } from '@silvermine/toolbox';
import { streamRecordsFromFile } from '../../lib/stream-records-from-file';
import { quitWithError } from '../../lib/quit-with-error';
import createWriteStream from '../../lib/create-write-stream';

interface CommandOptions {
   input: string;
   output?: string;
   region?: string;
}

interface ShardBatchRecord {
   [key: string]: unknown;
   shardId: string;
   startSequenceNumber: string;
   endSequenceNumber: string;
   streamArn: string;
}

function isShardBatchRecord(value: Record<string, unknown>): value is ShardBatchRecord {
   return isString(value.shardId)
      && isString(value.startSequenceNumber)
      && isString(value.endSequenceNumber)
      && isString(value.streamArn);
}

async function createOutputWriter(outputFile?: string): Promise<{ write: (line: string) => void; close: () => void }> {
   if (outputFile) {
      const stream = await createWriteStream(outputFile);

      return {
         write: (line: string) => { stream.write(line + '\n'); },
         close: () => { stream.close(); },
      };
   }

   return {
      write: console.info,
      close: () => {}, // eslint-disable-line no-empty-function
   };
}

async function fetchShardRecords(
   client: DynamoDBStreamsClient,
   batch: ShardBatchRecord,
   writeLine: (line: string) => void
): Promise<number> {
   const iteratorResp = await client.send(new GetShardIteratorCommand({
      StreamArn: batch.streamArn,
      ShardId: batch.shardId,
      ShardIteratorType: ShardIteratorType.AT_SEQUENCE_NUMBER,
      SequenceNumber: batch.startSequenceNumber,
   }));

   let shardIterator = iteratorResp.ShardIterator,
       count = 0,
       done = false;

   while (shardIterator && !done) {
      const response = await client.send(new GetRecordsCommand({ ShardIterator: shardIterator }));

      for (const record of response.Records ?? []) {
         const seqNum = record.dynamodb?.SequenceNumber;

         if (seqNum && BigInt(seqNum) > BigInt(batch.endSequenceNumber)) {
            done = true;
            break;
         }

         writeLine(JSON.stringify(record));
         count += 1;
      }

      shardIterator = response.NextShardIterator;
   }

   return count;
}

async function getShardRecords(this: Command, opts: CommandOptions): Promise<void> {
   const client = new DynamoDBStreamsClient({ region: opts.region }),
         outputWriter = await createOutputWriter(opts.output);

   let totalCount = 0;

   for await (const rawRecord of streamRecordsFromFile(opts.input)) {
      if (!isShardBatchRecord(rawRecord)) {
         quitWithError(
            `Input record missing fields (shardId, startSequenceNumber, endSequenceNumber, streamArn): ${JSON.stringify(rawRecord)}`
         );
      }

      const count = await fetchShardRecords(client, rawRecord, outputWriter.write);

      totalCount += count;

      if (opts.output) {
         console.error(`Fetched ${count} records from shard ${rawRecord.shardId}`);
      }
   }

   outputWriter.close();

   if (opts.output) {
      console.error(`Total: ${totalCount} records`);
   }
}

export default function register(command: Command): void {
   /* eslint-disable @silvermine/silvermine/call-indentation */
   command
      .description('Fetches the DynamoDB stream records from failed Lambda shard batches')
      .requiredOption('-i, --input <path>', 'path to the input file of shard batch records (.ndjson, .jsonl, .csv, or .tsv)')
      .option('-o, --output <path>', 'file to write the stream records (default: stdout)')
      .option('--region <value>', 'region to send requests to')
      .action(getShardRecords);
   /* eslint-enable @silvermine/silvermine/call-indentation */
}
