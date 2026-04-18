import { InvocationType, LambdaClient } from '@aws-sdk/client-lambda';
import PQueue from 'p-queue';
import chalk from 'chalk';
import { Flags } from '@oclif/core';
import { generateDefaultOutputFilename } from '../../lib/generate-default-output-filename.js';
import createWriteStream from '../../lib/create-write-stream.js';
import { invokeLambdaFunction } from '../../lib/aws/invoke-lambda-function.js';
import { streamLinesFromFile } from '../../lib/stream-lines-from-file.js';
import { BaseCommand } from '../../base-command.js';

export default class BulkInvoke extends BaseCommand {

   public static summary = 'Invoke a Lambda function with payloads from a file';

   public static flags = {
      name: Flags.string({
         description: 'name of the Lambda function',
         required: true,
      }),
      'payloads-file': Flags.string({
         char: 'i',
         description: 'name of the file containing newline-delimited payloads',
         required: true,
      }),
      failed: Flags.string({
         description: 'name of the file to write the responses from failed invocations',
      }),
      successful: Flags.string({
         description: 'name of the file to write the response from successful invocations',
      }),
      'json-decode': Flags.boolean({
         description: 'attempt to JSON decode the response payload',
         default: true,
         allowNo: true,
      }),
      'invocation-type': Flags.custom<InvocationType>({
         description: 'invoke the function synchronously or asynchronously',
         options: Object.values(InvocationType),
         default: InvocationType.RequestResponse,
      })(),
      concurrency: Flags.integer({
         description: 'number of concurrent invocation requests',
         default: 10,
      }),
   };

   public async run(): Promise<void> {
      const { flags } = await this.parse(BulkInvoke),
            lambda = new LambdaClient({ region: flags.region }),
            queue = new PQueue({ concurrency: flags.concurrency }),
            maxQueueSize = flags.concurrency * 5,
            failedInvocationsFile = flags.failed || generateDefaultOutputFilename('lambda', 'bulk-invoke', flags.name, 'failed');

      const successfulInvocationsFile = flags.successful
         || generateDefaultOutputFilename('lambda', 'bulk-invoke', flags.name, 'successful');

      const invocationType = flags['invocation-type'];

      const failedPayloadsWriteStream = await createWriteStream(failedInvocationsFile),
            successfulInvocationsWriteStream = await createWriteStream(successfulInvocationsFile),
            counters = { failed: 0, successful: 0 };

      this.log(chalk.yellow(
         `Starting bulk invocation of ${flags.name} from ${flags['payloads-file']}`
         + ` (type: ${flags['invocation-type']}, concurrency: ${flags.concurrency})`
      ));
      this.log(`${chalk.gray('Successful output:')} ${successfulInvocationsFile}`);
      this.log(`${chalk.gray('Failed output:')} ${failedInvocationsFile}`);

      for await (const payload of streamLinesFromFile(flags['payloads-file'])) {
         queue.add(async () => {
            const resp = await invokeLambdaFunction(lambda, {
               name: flags.name,
               invocationType,
               payload,
            });

            let responsePayload = resp.responsePayload;

            if (flags['json-decode'] && responsePayload) {
               try {
                  responsePayload = JSON.parse(responsePayload);
               } catch(_e) {
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
               this.log(chalk.gray(`Status: ${counters.successful} successful / ${counters.failed} failed`));
            }
         });

         if (queue.size > maxQueueSize) {
            await queue.onEmpty();
         }
      }

      await queue.onIdle();

      successfulInvocationsWriteStream.end();
      failedPayloadsWriteStream.end();

      this.log(chalk.whiteBright(
         `Total: ${counters.successful + counters.failed} invocations (${counters.successful} successful / ${counters.failed} failed)`
      ));

      if (counters.failed > 0) {
         this.exit(1);
      }
   }

}
