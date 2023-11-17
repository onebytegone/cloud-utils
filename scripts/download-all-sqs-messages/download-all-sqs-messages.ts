import minimist from 'minimist';
import { getRequiredArg } from '../../lib/minimist/get-required-arg';
import { DeleteMessageBatchCommand, GetQueueUrlCommand, ReceiveMessageCommand, SQSClient } from '@aws-sdk/client-sqs';
import { createWriteStream } from 'fs';
import { DateTime } from 'luxon';
import { dirname, join } from 'path';
import { mkdir } from 'fs/promises';

const sqs = new SQSClient({}),
      argv = minimist(process.argv, { string: [ 'queue-name', 'output' ], boolean: [ 'dry-run', 'delete' ] }),
      queueName = getRequiredArg(argv, 'queue-name'),
      outputFile = argv.output ? argv.output : join('output', `${queueName}-${DateTime.utc().toFormat('yyyy-LL-dd\'T\'HHmmss')}.json`);

(async () => {
   const queueURL = (await sqs.send(new GetQueueUrlCommand({ QueueName: queueName }))).QueueUrl;

   console.info(`Downloading messages from: ${queueURL} to ${outputFile}`);

   await mkdir(dirname(outputFile), { recursive: true });

   const writeStream = createWriteStream(outputFile, 'utf-8');

   async function processBatch(): Promise<number> {
      const messages = await sqs.send(new ReceiveMessageCommand({ QueueUrl: queueURL, MaxNumberOfMessages: 10 }));

      messages.Messages?.forEach((message) => {
         writeStream.write(JSON.stringify(message) + '\n');
      });

      if (argv.delete && messages.Messages?.length) {
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

      return messages.Messages && messages.Messages.length || 0;
   }

   await new Promise<void>((resolve, reject) => {
      async function loop(): Promise<void> {
         try {
            const numberOfMessagesProcessed = await processBatch();

            if (numberOfMessagesProcessed === 0) {
               console.info('No messages appear to be left, finishing up...');
               resolve();
               return;
            } else {
               console.info(`Processed ${numberOfMessagesProcessed} messages...`);
            }

            setTimeout(loop, 1000);
         } catch(e) {
            reject(e);
         }
      }

      setTimeout(loop, 1);
   });

   writeStream.close();
})();
