import { DeleteMessageBatchCommand, GetQueueUrlCommand, ReceiveMessageCommand, SQSClient } from '@aws-sdk/client-sqs';
import { Flags } from '@oclif/core';
import { generateDefaultOutputFilename } from '../../lib/generate-default-output-filename.js';
import createWriteStream from '../../lib/create-write-stream.js';
import endWriteStream from '../../lib/end-write-stream.js';
import { delay } from '@silvermine/toolbox';
import { BaseCommand } from '../../base-command.js';

export default class DownloadMessages extends BaseCommand {

   public static summary = 'Download all available messages from an SQS queue';

   public static description = 'Downloads all available messages from an SQS queue.'
      + ' WARNING: if this script is not running with --delete and it runs'
      + ' longer than the queue\'s visibility timeout, all messages may not'
      + ' be downloaded.';

   public static flags = {
      name: Flags.string({
         description: 'name of the SQS queue',
         required: true,
      }),
      output: Flags.string({
         char: 'o',
         description: 'name of the file to write the messages',
      }),
      delete: Flags.boolean({
         description: 'if supplied, the messages will be DELETED from the queue',
         default: false,
      }),
      concurrency: Flags.integer({
         description: 'number of concurrent batches to fetch',
         default: 10,
      }),
   };

   public async run(): Promise<void> {
      const { flags } = await this.parse(DownloadMessages),
            sqs = new SQSClient({ region: flags.region });

      const queueURL = (await sqs.send(new GetQueueUrlCommand({ QueueName: flags.name }))).QueueUrl;

      if (!queueURL) {
         this.error(`Could not find SQS queue with name "${flags.name}"`);
      }

      const outputFile = flags.output || generateDefaultOutputFilename('sqs', 'download-messages', flags.name);

      this.log(`Downloading messages from ${queueURL} to ${outputFile}`);

      const writeStream = await createWriteStream(outputFile),
            processedMessages = new Set<string>();

      const processBatch = async (): Promise<number> => {
         const messages = await sqs.send(new ReceiveMessageCommand({ QueueUrl: queueURL, MaxNumberOfMessages: 10 }));

         const newMessageCount = (messages.Messages || []).reduce((memo, message) => {
            if (message.MessageId && !processedMessages.has(message.MessageId)) {
               processedMessages.add(message.MessageId);
               writeStream.write(JSON.stringify(message) + '\n');
               return memo + 1;
            }

            return memo;
         }, 0);

         if (flags.delete && messages.Messages?.length) {
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
            this.log(`${processedMessages.size} messages downloaded...`);
         }

         return newMessageCount;
      };

      const counts = await Promise.all(Array.from(Array(flags.concurrency).keys()).map(async (workerID) => {
         let count = 0,
             emptyReceiveCount = 0;

         for (;;) {
            const newMessagesFound = await processBatch();

            count += newMessagesFound;

            if (!newMessagesFound && emptyReceiveCount >= 3) {
               this.log(`No new messages appear to be left, finished. (worker ${workerID})`);
               break;
            } else if (!newMessagesFound) {
               emptyReceiveCount += 1;
               this.log(`No new messages appear to be left, waiting... (worker ${workerID})`);
               await delay(10);
            }
         }

         return count;
      }));

      this.log(`Downloaded ${counts.reduce((memo, v) => { return memo + v; }, 0)} messages`);

      await endWriteStream(writeStream);
   }

}
