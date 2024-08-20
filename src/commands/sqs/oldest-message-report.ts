import { GetQueueAttributesCommand, ListQueuesCommand, SQSClient } from '@aws-sdk/client-sqs';
import { Command } from 'commander';
import { quitWithError } from '../../lib/quit-with-error';
import { CloudWatchClient, GetMetricStatisticsCommand } from '@aws-sdk/client-cloudwatch';
import { isEmpty, isNotNullOrUndefined, isUndefined } from '@silvermine/toolbox';
import { Duration } from 'luxon';
import chalk from 'chalk';
import { table } from 'table';

interface CommandOptions {
   region?: string;
   ignoreEmpty: boolean;
}

async function generateOldestMessageReport(this: Command, opts: CommandOptions): Promise<void> {
   const sqs = new SQSClient({ region: opts.region }),
         cw = new CloudWatchClient({ region: opts.region }),
         listResp = await sqs.send(new ListQueuesCommand({}));

   if (listResp.NextToken) {
      quitWithError('ERROR: pagination is not supported');
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
         console.warn(`Could not find queue ${queueURL}`);
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

      const datapoints = oldestMessageMetricResp.Datapoints || [];

      records.push({
         queue: queueName,
         messages,
         secondsToLoss: isEmpty(datapoints) ? undefined : retentionSeconds - (datapoints[datapoints.length - 1].Maximum || 0),
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
      if (opts.ignoreEmpty && record.messages === 0) {
         return undefined;
      }

      if (isUndefined(record.secondsToLoss)) {
         return [
            chalk.grey(record.queue),
            chalk.grey(record.messages),
            chalk.grey('n/a'),
         ];
      }

      const humanDuration = Duration.fromMillis(record.secondsToLoss * 1000)
         .shiftTo('days', 'hours')
         .toHuman({ maximumFractionDigits: 0 });

      let color: keyof typeof chalk = 'white';

      if (record.secondsToLoss < 7 * 24 * 60) { // 7 days
         color = 'yellow';
      }

      if (record.secondsToLoss < 3 * 24 * 60) { // 3 days
         color = 'red';
      }

      return [
         chalk[color](record.queue),
         chalk[color](record.messages),
         chalk[color](`${humanDuration} (${record.secondsToLoss.toLocaleString()} seconds)`),
      ];
   });

   console.info(table([
      [ chalk.bold('Queue'), chalk.bold('Messages'), chalk.bold('Time to message loss') ],
      ...rows.filter(isNotNullOrUndefined),
   ]));
}

export default function register(command: Command): void {
   command
      .description('Generates a report of which SQS queues have messages nearing expiration')
      .option('--region <value>', 'Region to send requests to')
      .option('--ignore-empty', 'Do not log queues that do not contain messages')
      .action(generateOldestMessageReport);
}
