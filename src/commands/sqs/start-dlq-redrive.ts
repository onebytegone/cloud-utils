import {
   GetQueueAttributesCommand,
   GetQueueUrlCommand,
   ListDeadLetterSourceQueuesCommand,
   SQSClient,
   StartMessageMoveTaskCommand,
} from '@aws-sdk/client-sqs';
import { Flags } from '@oclif/core';
import { isEmpty } from '@silvermine/toolbox';
import chalk from 'chalk';
import { BaseCommand } from '../../base-command.js';

async function listAllDLQSourceQueueURLs(sqs: SQSClient, dlqURL: string): Promise<string[]> {
   const urls: string[] = [];

   let nextToken: string | undefined;

   for (;;) {
      const resp = await sqs.send(new ListDeadLetterSourceQueuesCommand({
         QueueUrl: dlqURL,
         NextToken: nextToken,
      }));

      for (const url of (resp.queueUrls || [])) {
         urls.push(url);
      }

      if (!resp.NextToken) {
         break;
      }

      nextToken = resp.NextToken;
   }

   return urls;
}

async function getQueueARNFromURL(sqs: SQSClient, queueURL: string): Promise<string | undefined> {
   const resp = await sqs.send(new GetQueueAttributesCommand({
      QueueUrl: queueURL,
      AttributeNames: [ 'QueueArn' ],
   }));

   return resp.Attributes?.QueueArn;
}

export default class StartDLQRedrive extends BaseCommand {

   public static summary = 'Start an SQS DLQ redrive (move messages from the DLQ back to a source queue)';

   public static description = 'Starts a message move task that redrives messages from the named DLQ.'
      + ' By default, the destination is auto-resolved from the DLQ\'s source-queue list and'
      + ' must be exactly one queue; pass --destination <name> to override.';

   public static flags = {
      name: Flags.string({
         description: 'name of the dead-letter queue to redrive from',
         required: true,
      }),
      destination: Flags.string({
         description: 'name of the destination queue (omit to auto-resolve from the DLQ\'s source-queue list)',
      }),
      'max-rate': Flags.integer({
         description: 'MaxNumberOfMessagesPerSecond for the move task; omit for AWS system-optimized rate',
         min: 1,
      }),
   };

   public async run(): Promise<void> {
      const { flags } = await this.parse(StartDLQRedrive),
            sqs = new SQSClient({ region: flags.region });

      const dlqURL = await this.resolveQueueURL(sqs, flags.name),
            dlqARN = await this.resolveQueueARNFromURL(sqs, dlqURL, flags.name);

      const destARN = await this.resolveDestinationARN(sqs, dlqURL, flags.name, flags.destination);

      const rateLabel = flags['max-rate'] === undefined
         ? 'system optimized'
         : String(flags['max-rate']);

      this.logInfoToStderr(chalk.gray(`Redriving from ${dlqARN} to ${destARN} (rate: ${rateLabel}).`));

      const moveResp = await sqs.send(new StartMessageMoveTaskCommand({
         SourceArn: dlqARN,
         DestinationArn: destARN,
         MaxNumberOfMessagesPerSecond: flags['max-rate'],
      }));

      this.log(JSON.stringify({ taskHandle: moveResp.TaskHandle }));

      this.logInfoToStderr(chalk.green('Move task started.'));
   }

   private async resolveDestinationARN(
      sqs: SQSClient,
      dlqURL: string,
      dlqName: string,
      destinationName: string | undefined
   ): Promise<string> {
      if (destinationName !== undefined) {
         const destURL = await this.resolveQueueURL(sqs, destinationName);

         return await this.resolveQueueARNFromURL(sqs, destURL, destinationName);
      }

      const sourceURLs = await listAllDLQSourceQueueURLs(sqs, dlqURL);

      if (isEmpty(sourceURLs)) {
         this.error(`No source queues reference DLQ "${dlqName}". Pass --destination <queue> to redrive into a specific queue.`);
      }

      if (sourceURLs.length > 1) {
         const list = sourceURLs.map((url) => { return `  - ${url}`; }).join('\n');

         this.error(`DLQ "${dlqName}" has ${sourceURLs.length} source queues:\n${list}\nPass --destination <queue> to disambiguate.`);
      }

      return await this.resolveQueueARNFromURL(sqs, sourceURLs[0], sourceURLs[0]);
   }

   private async resolveQueueURL(sqs: SQSClient, queueName: string): Promise<string> {
      const resp = await sqs.send(new GetQueueUrlCommand({ QueueName: queueName }));

      if (!resp.QueueUrl) {
         this.error(`Could not find SQS queue with name "${queueName}"`);
      }

      return resp.QueueUrl;
   }

   private async resolveQueueARNFromURL(sqs: SQSClient, queueURL: string, label: string): Promise<string> {
      const arn = await getQueueARNFromURL(sqs, queueURL);

      if (!arn) {
         this.error(`Could not resolve ARN for queue "${label}"`);
      }

      return arn;
   }

}
