import { InvocationType, LambdaClient } from '@aws-sdk/client-lambda';
import { Command, Option } from 'commander';
import PQueue from 'p-queue';
import { generateDefaultOutputFilename } from '../../../lib/generate-default-output-filename';
import createWriteStream from '../../../lib/create-write-stream';
import { invokeLambdaFunction } from '../../../lib/aws/invoke-lambda-function';
import chalk from 'chalk';
import { streamLinesFromFile } from '../../../lib/stream-lines-from-file';

interface CommandOptions {
   name: string;
   payloadsFile: string;
   failed?: string;
   successful?: string;
   jsonDecode: boolean;
   invocationType: InvocationType;
   concurrency: number;
}

const lambda = new LambdaClient({});

async function bulkInvokeLambdaFunction(this: Command, opts: CommandOptions): Promise<void> {
   const queue = new PQueue({ concurrency: opts.concurrency }),
         maxQueueSize = opts.concurrency * 5,
         failedInvocationsFile = opts.failed || generateDefaultOutputFilename('lambda', 'bulk-invoke', opts.name, 'failed'),
         successfulInvocationsFile = opts.successful || generateDefaultOutputFilename('lambda', 'bulk-invoke', opts.name, 'successful'),
         failedPayloadsWriteStream = await createWriteStream(failedInvocationsFile),
         successfulInvocationsWriteStream = await createWriteStream(successfulInvocationsFile),
         counters = { failed: 0, successful: 0 };

   console.info(chalk.yellow(
      `Starting bulk invocation of ${opts.name} from ${opts.payloadsFile} (type: ${opts.invocationType}, concurrency: ${opts.concurrency})`
   ));
   console.info(`${chalk.gray('Successful output:')} ${successfulInvocationsFile}`);
   console.info(`${chalk.gray('Failed output:')} ${failedInvocationsFile}`);

   for await (const payloads of streamLinesFromFile(opts.payloadsFile)) {
      payloads.forEach((payload) => {
         queue.add(async () => {
            const resp = await invokeLambdaFunction(lambda, {
               name: opts.name,
               invocationType: opts.invocationType,
               payload,
            });

            let responsePayload = resp.responsePayload;

            if (opts.jsonDecode && responsePayload) {
               try {
                  responsePayload = JSON.parse(responsePayload);
               } catch(e) {
                  // noop
               }
            }

            if (resp.error) {
               counters.failed += 1;
               failedPayloadsWriteStream.write(JSON.stringify({
                  payload,
                  error: resp.error,
                  responsePayload,
               }) + '\n');
            } else {
               counters.successful += 1;
               successfulInvocationsWriteStream.write(JSON.stringify({
                  payload,
                  responsePayload,
               }) + '\n');
            }

            if ((counters.successful + counters.failed) % 10 === 0) {
               console.info(chalk.gray(`Status: ${counters.successful} successful / ${counters.failed} failed`));
            }
         });
      });

      if (queue.size > maxQueueSize) {
         await queue.onEmpty();
      }
   }

   await queue.onIdle();

   console.info(chalk.whiteBright(
      `Total: ${counters.successful + counters.failed} invocations (${counters.successful} successful / ${counters.failed} failed)`
   ));

   successfulInvocationsWriteStream.close();
   failedPayloadsWriteStream.close();
}

export default function register(command: Command): void {
   /* eslint-disable @silvermine/silvermine/call-indentation */
   command
      .description(
         'Invokes a Lambda function with the provided payloads'
      )
      .requiredOption('--name <string>', 'name of the Lambda function')
      .requiredOption('-i, --payloads-file <path>', 'name of the file containing newline-delimited payloads')
      .option('--failed <path>', 'name of the file to write the responses from failed invocations')
      .option('--successful <path>', 'name of the file to write the response from successful invocations')
      .option('--no-json-decode', 'Don\'t attempt to JSON decode the function\'s response payload')
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
      .action(bulkInvokeLambdaFunction);
   /* eslint-enable @silvermine/silvermine/call-indentation */
}
