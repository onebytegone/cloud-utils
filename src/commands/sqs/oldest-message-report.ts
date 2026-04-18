import { GetQueueAttributesCommand, ListQueuesCommand, SQSClient } from '@aws-sdk/client-sqs';
import { CloudWatchClient, GetMetricStatisticsCommand } from '@aws-sdk/client-cloudwatch';
import { isEmpty, isNotNullOrUndefined, isUndefined } from '@silvermine/toolbox';
import { Duration } from 'luxon';
import chalk from 'chalk';
import { table } from 'table';
import { Flags } from '@oclif/core';
import { BaseCommand } from '../../base-command.js';

const SEVEN_DAYS_IN_SECONDS = 7 * 24 * 60 * 60,
      THREE_DAYS_IN_SECONDS = 3 * 24 * 60 * 60;

export default class OldestMessageReport extends BaseCommand {

   public static summary = 'Report which SQS queues have messages nearing expiration';

   public static flags = {
      'ignore-empty': Flags.boolean({
         description: 'Do not log queues that do not contain messages',
      }),
   };

   public async run(): Promise<void> {
      const { flags } = await this.parse(OldestMessageReport),
            sqs = new SQSClient({ region: flags.region }),
            cw = new CloudWatchClient({ region: flags.region }),
            listResp = await sqs.send(new ListQueuesCommand({}));

      if (listResp.NextToken) {
         this.error('Pagination is not supported');
      }

      const records: { queue: string; messages: number; secondsToLoss?: number }[] = [];

      for (const queueURL of (listResp.QueueUrls || [])) {
         const attrResp = await sqs.send(new GetQueueAttributesCommand({
            QueueUrl: queueURL,
            AttributeNames: [
               'ApproximateNumberOfMessages',
               'MessageRetentionPeriod',
               'QueueArn',
            ],
         }));

         if (!attrResp.Attributes) {
            this.warn(`Could not find queue ${queueURL}`);
            continue;
         }

         const retentionSeconds = Number(attrResp.Attributes.MessageRetentionPeriod),
               messages = Number(attrResp.Attributes.ApproximateNumberOfMessages),
               queueName = (attrResp.Attributes.QueueArn || '').replace(/^.*:/, '');

         if (messages === 0) {
            records.push({
               queue: queueName,
               messages,
            });
            continue;
         }

         const oldestMessageMetricResp = await cw.send(new GetMetricStatisticsCommand({
            Namespace: 'AWS/SQS',
            MetricName: 'ApproximateAgeOfOldestMessage',
            Dimensions: [
               { Name: 'QueueName', Value: queueName },
            ],
            Statistics: [
               'Maximum',
            ],
            StartTime: new Date(Date.now() - (3 * 60 * 1000)),
            EndTime: new Date(),
            Period: 60,
         }));

         const datapoints = (oldestMessageMetricResp.Datapoints || [])
            .sort((a, b) => {
               return (b.Timestamp?.getTime() || 0) - (a.Timestamp?.getTime() || 0);
            });

         records.push({
            queue: queueName,
            messages,
            secondsToLoss: isEmpty(datapoints) ? undefined : retentionSeconds - (datapoints[0].Maximum || 0),
         });
      }

      records.sort((a, b) => {
         if (a.secondsToLoss !== b.secondsToLoss) {
            return (isUndefined(a.secondsToLoss) ? Number.MAX_SAFE_INTEGER : a.secondsToLoss)
               - (isUndefined(b.secondsToLoss) ? Number.MAX_SAFE_INTEGER : b.secondsToLoss);
         }

         if (a.messages !== b.messages) {
            return b.messages - a.messages;
         }

         return a.queue.localeCompare(b.queue);
      });

      const rows = records.map((record) => {
         if (flags['ignore-empty'] && record.messages === 0) {
            return undefined;
         }

         if (isUndefined(record.secondsToLoss)) {
            return [
               chalk.gray(record.queue),
               chalk.gray(record.messages),
               chalk.gray('n/a'),
            ];
         }

         const humanDuration = Duration.fromMillis(record.secondsToLoss * 1000)
            .shiftTo('days', 'hours')
            .toHuman({ maximumFractionDigits: 0 });

         let color: keyof typeof chalk = 'white';

         if (record.secondsToLoss < SEVEN_DAYS_IN_SECONDS) {
            color = 'yellow';
         }

         if (record.secondsToLoss < THREE_DAYS_IN_SECONDS) {
            color = 'red';
         }

         return [
            chalk[color](record.queue),
            chalk[color](record.messages),
            chalk[color](`${humanDuration} (${record.secondsToLoss.toLocaleString()} seconds)`),
         ];
      });

      this.log(table([
         [ chalk.bold('Queue'), chalk.bold('Messages'), chalk.bold('Time to message loss') ],
         ...rows.filter(isNotNullOrUndefined),
      ]));
   }

}
