import { InvocationType, InvokeCommand, LambdaClient } from '@aws-sdk/client-lambda';
import { Command, Option } from 'commander';
import PQueue from 'p-queue';
import { quitWithError } from '../../../lib/quit-with-error';
import { generateDefaultOutputFilename } from '../../../lib/generate-default-output-filename';
import createWriteStream from '../../../lib/create-write-stream';
import { createReadStream } from 'fs';
import readline from 'readline';

interface CommandOptions {
   name: string;
   failed?: string;
   sourceQueue?: string;
   sourceFile?: string;
   invocationType: InvocationType;
   concurrency: number;
}

const lambda = new LambdaClient({});

async function* streamLinesFromFile(file: string): AsyncIterable<string[]> {
   const rl = readline.createInterface({
      input: createReadStream(file),
      crlfDelay: Infinity,
   });

   for await (const line of rl) {
      yield [ line ]; // eslint-disable-line no-restricted-syntax
   }
}

function makeIterator(opts: CommandOptions): () => AsyncIterable<string[]> {
   if (opts.sourceFile) {
      return streamLinesFromFile.bind(undefined, opts.sourceFile);
   }

   quitWithError(`Unable to create iterator ${JSON.stringify(opts)}`);
}

async function batchInvokeLambdaFunction(this: Command, opts: CommandOptions): Promise<void> {
   const queue = new PQueue({ concurrency: opts.concurrency }),
         maxQueueSize = opts.concurrency * 5,
         failedPayloadsFile = opts.failed || generateDefaultOutputFilename('lambda', 'batch-invoke', opts.name, 'failed'),
         failedPayloadsWriteStream = await createWriteStream(failedPayloadsFile),
         iterator = makeIterator(opts);

   for await (const payloads of iterator()) {
      payloads.forEach((payload) => {
         queue.add(async () => {
            try {
               await lambda.send(new InvokeCommand({
                  FunctionName: opts.name,
                  InvocationType: opts.invocationType,
                  Payload: payload,
               }));
            } catch(e) {
               if (e instanceof Error) {
                  console.error(
                     `Invocation of ${opts.name} failed with "${e.message}" (${e.name}). Failed payload written to ${failedPayloadsFile}`
                  );
                  failedPayloadsWriteStream.write(payload + '\n');
               } else {
                  throw e;
               }
            }
         });
      });

      if (queue.size > maxQueueSize) {
         await queue.onEmpty();
      }
   }

   await queue.onIdle();
   failedPayloadsWriteStream.close();
}

export default function register(command: Command): void {
   /* eslint-disable @silvermine/silvermine/call-indentation */
   command
      .description(
         'Invokes a Lambda function with the provided payloads'
      )
      .requiredOption('--name <string>', 'name of the Lambda function')
      .requiredOption('--source-file <string>', 'name of the file  to retrieve payloads from')
      .option('--failed <string>', 'name of the file to write the payloads from failed invocations')
      .addOption(
         new Option('--invocation-type <string>', 'invoke the function synchronously or asynchronously')
            .choices(Object.values(InvocationType))
            .default(InvocationType.RequestResponse)
      )
      .addOption(
         new Option('--concurrency <number>', 'number of concurrent invocation requests')
            .argParser((value) => {
               return Number(value);
            })
            .default(10)
      )
      .action(batchInvokeLambdaFunction);
   /* eslint-enable @silvermine/silvermine/call-indentation */
}
