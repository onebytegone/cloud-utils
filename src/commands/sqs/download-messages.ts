import { DeleteMessageBatchCommand, GetQueueUrlCommand, ReceiveMessageCommand, SQSClient } from '@aws-sdk/client-sqs';
import { Command, Option } from 'commander';
import { quitWithError } from '../../lib/quit-with-error';
import { generateDefaultOutputFilename } from '../../lib/generate-default-output-filename';
import createWriteStream from '../../lib/create-write-stream';
import { delay } from '@silvermine/toolbox';

interface CommandOptions {
   name: string;
   delete: boolean;
   concurrency: number;
   output?: string;
   region?: string;
}

async function downloadMessages(this: Command, opts: CommandOptions): Promise<void> {
   const sqs = new SQSClient({ region: opts.region });

   const queueURL = (await sqs.send(new GetQueueUrlCommand({ QueueName: opts.name }))).QueueUrl;

   if (!queueURL) {
      quitWithError(`Could not find SQS queue with name "${opts.name}"`);
   }

   const outputFile = opts.output || generateDefaultOutputFilename('sqs', 'download-messages', opts.name);

   console.info(`Downloading messages from ${queueURL} to ${outputFile}`);

   const writeStream = await createWriteStream(outputFile),
         processedMessages = new Set<string>();

   async function processBatch(): Promise<number> {
      const messages = await sqs.send(new ReceiveMessageCommand({ QueueUrl: queueURL, MaxNumberOfMessages: 10 }));

      const newMessageCount = (messages.Messages || []).reduce((memo, message) => {
         if (message.MessageId && !processedMessages.has(message.MessageId)) {
            processedMessages.add(message.MessageId);
            writeStream.write(JSON.stringify(message) + '\n');
            return memo + 1;
         }

         return memo;
      }, 0);

      if (opts.delete && messages.Messages?.length) {
         await sqs.send(new DeleteMessageBatchCommand({
            QueueUrl: queueURL,
            Entries: messages.Messages?.map((message) => {
               return {
                  Id: message.MessageId,
                  ReceiptHandle: message.ReceiptHandle,
               };
            }),
         }));
      }

      if (processedMessages.size > 0 && processedMessages.size % 100 === 0) {
         console.info(`${processedMessages.size} messages downloaded...`);
      }

      return newMessageCount;
   }

   const counts = await Promise.all(Array.from(Array(opts.concurrency).keys()).map(async (workerID) => {
      let count = 0,
          emptyReceiveCount = 0;

      for (;;) {
         const newMessagesFound = await processBatch();

         count += newMessagesFound;

         if (!newMessagesFound && emptyReceiveCount >= 3) {
            console.info(`No new messages appear to be left, finished. (worker ${workerID})`);
            break;
         } else if (!newMessagesFound) {
            emptyReceiveCount += 1;
            console.info(`No new messages appear to be left, waiting... (worker ${workerID})`);
            await delay(10);
         }
      }

      return count;
   }));

   console.info(`Downloaded ${counts.reduce((memo, v) => { return memo + v; }, 0)} messages`);

   writeStream.close();
}

export default function register(command: Command): void {
   /* eslint-disable @silvermine/silvermine/call-indentation */
   command
      .description(
         'Downloads all available messages from an SQS queue. WARNING: if this script is'
         + ' not running with --delete and it runs longer than the queue\'s visibility'
         + ' timeout, all messages may not be downloaded.'
      )
      .requiredOption('--name <string>', 'Name of the SQS queue')
      .option('-o, --output <string>', 'Name of the file to write the messages')
      .option('--region <value>', 'Region to send requests to')
      .addOption(
         new Option('--delete', 'If supplied, the messages will be DELETED from the queue')
            .default(false)
      )
      .addOption(
         new Option('--concurrency <number>', 'Number of concurrent batches to fetch')
            .argParser((value) => {
               return Number(value);
            })
            .default(10)
      )
      .action(downloadMessages);
   /* eslint-enable @silvermine/silvermine/call-indentation */
}
