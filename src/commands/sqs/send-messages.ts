import { GetQueueUrlCommand, SendMessageBatchCommand, SendMessageBatchRequestEntry, SQSClient } from '@aws-sdk/client-sqs';
import { WriteStream } from 'fs';
import { Command, Option } from 'commander';
import PQueue from 'p-queue';
import chalk from 'chalk';
import { generateDefaultOutputFilename } from '../../lib/generate-default-output-filename';
import createWriteStream from '../../lib/create-write-stream';
import { streamLinesFromFile } from '../../lib/stream-lines-from-file';
import { batchRecords } from '../../lib/batch-records';
import { quitWithError } from '../../lib/quit-with-error';

const BATCH_SIZE = 10,
      STATUS_INTERVAL = BATCH_SIZE * 10;

interface CommandOptions {
   name: string;
   messagesFile: string;
   failed?: string;
   concurrency: number;
   delaySeconds?: number;
   region?: string;
}

async function sendBatch(
   client: SQSClient,
   queueUrl: string,
   messages: string[],
   getFailedWriteStream: () => Promise<WriteStream>,
   counters: { sent: number; failed: number },
   delaySeconds?: number
): Promise<void> {
   const entries: SendMessageBatchRequestEntry[] = messages.map((message, index) => {
      return { Id: String(index), MessageBody: message, DelaySeconds: delaySeconds };
   });

   let response;

   try {
      response = await client.send(new SendMessageBatchCommand({
         QueueUrl: queueUrl,
         Entries: entries,
      }));
   } catch(e) {
      counters.failed += messages.length;
      const failedWriteStream = await getFailedWriteStream();

      messages.forEach((message) => {
         failedWriteStream.write(JSON.stringify({
            message,
            error: { code: 'BatchFailed', message: e instanceof Error ? e.message : String(e) },
         }) + '\n');
      });
      return;
   }

   counters.sent += (response.Successful || []).length;

   if ((response.Failed || []).length > 0) {
      const failedWriteStream = await getFailedWriteStream();

      for (const failed of response.Failed || []) {
         counters.failed += 1;
         failedWriteStream.write(JSON.stringify({
            message: messages[Number(failed.Id)],
            error: { code: failed.Code, message: failed.Message },
         }) + '\n');
      }
   }

   if ((counters.sent + counters.failed) % STATUS_INTERVAL === 0) {
      console.info(chalk.gray(`Status: ${counters.sent} sent / ${counters.failed} failed`));
   }
}

async function sendMessages(this: Command, opts: CommandOptions): Promise<void> {
   const client = new SQSClient({ region: opts.region });

   const queueUrl = (await client.send(new GetQueueUrlCommand({ QueueName: opts.name }))).QueueUrl;

   if (!queueUrl) {
      quitWithError(`Could not find SQS queue with name "${opts.name}"`);
   }

   const failedFile = opts.failed || generateDefaultOutputFilename('sqs', 'send-messages', opts.name, 'failed'),
         queue = new PQueue({ concurrency: opts.concurrency }),
         maxQueueSize = opts.concurrency * 5,
         counters = { sent: 0, failed: 0 };

   let failedWriteStreamPromise: Promise<WriteStream> | undefined;

   function getFailedWriteStream(): Promise<WriteStream> {
      if (!failedWriteStreamPromise) {
         failedWriteStreamPromise = createWriteStream(failedFile);
      }

      return failedWriteStreamPromise;
   }

   console.info(chalk.yellow(`Sending messages from ${opts.messagesFile} to "${opts.name}"`));

   for await (const messages of batchRecords<string>(streamLinesFromFile(opts.messagesFile), BATCH_SIZE)) {
      queue.add(async () => { return sendBatch(client, queueUrl, messages, getFailedWriteStream, counters, opts.delaySeconds); });

      if (queue.size > maxQueueSize) {
         await queue.onEmpty();
      }
   }

   await queue.onIdle();

   console.info(chalk.whiteBright(
      `Total: ${counters.sent + counters.failed} messages (${counters.sent} sent / ${counters.failed} failed)`
   ));

   if (failedWriteStreamPromise) {
      (await failedWriteStreamPromise).close();
      console.info(`${chalk.gray('Failed output:')} ${failedFile}`);
   }
}

export default function register(command: Command): void {
   /* eslint-disable @silvermine/silvermine/call-indentation */
   command
      .description('Sends messages to an SQS queue from a newline-delimited file')
      .requiredOption('--name <string>', 'name of the SQS queue')
      .requiredOption('-i, --messages-file <path>', 'path to the file containing newline-delimited messages')
      .option('--failed <path>', 'name of the file to write failed messages')
      .option('--region <value>', 'Region to send requests to')
      .addOption(
         new Option('--concurrency <number>', 'number of concurrent SendMessageBatch requests')
            .argParser((value) => { return Number(value); })
            .default(10)
      )
      .addOption(
         new Option('--delay-seconds <number>', 'number of seconds to delay message delivery (0-900)')
            .argParser((value) => { return Number(value); })
      )
      .action(sendMessages);
   /* eslint-enable @silvermine/silvermine/call-indentation */
}
