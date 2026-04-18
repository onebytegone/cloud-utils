import {
   CloudWatchLogsClient,
   FilterLogEventsCommand,
   FilterLogEventsCommandInput,
   FilterLogEventsCommandOutput,
   FilteredLogEvent,
} from '@aws-sdk/client-cloudwatch-logs';
import chalk from 'chalk';
import { Flags } from '@oclif/core';
import { BaseCommand } from '../../base-command.js';

interface LogQueryParams {
   logGroupName: string;
   startTime: number;
   endTime?: number;
   filterPattern?: string;
   nextToken?: string;
}

interface LogEvent {
   timestamp: number;
   message: string;
   logStreamName: string;
}

interface LogQueryResult {
   events: LogEvent[];
   nextToken?: string;
}

function buildQueryParams(opts: {
   functionName: string;
   minutes: number;
   filter?: string;
   nextToken?: string;
}): LogQueryParams {
   const logGroupName = `/aws/lambda/${opts.functionName}`,
         startTime = Date.now() - (opts.minutes * 60 * 1000);

   const params: LogQueryParams = {
      logGroupName,
      startTime,
   };

   if (opts.filter) {
      params.filterPattern = opts.filter;
   }

   if (opts.nextToken) {
      params.nextToken = opts.nextToken;
   }

   return params;
}

function formatLogEvent(event: LogEvent): string {
   const timestamp = new Date(event.timestamp).toISOString(),
         formattedTimestamp = chalk.gray(`[${timestamp}]`);

   let message = event.message;

   if (message.includes('ERROR')) {
      message = chalk.red(message);
   } else if (message.includes('WARN')) {
      message = chalk.yellow(message);
   }

   return `${formattedTimestamp} ${message}`;
}

function handleAWSError(error: unknown, logGroupName?: string): never {
   const errorName = (error instanceof Error) ? error.name : '',
         errorMessage = (error instanceof Error) ? error.message : '';

   if (errorName === 'ResourceNotFoundException') {
      const groupInfo = logGroupName ? ` (${logGroupName})` : '';

      throw new Error(`Function not found or has no log group${groupInfo}`);
   }

   if (errorName === 'AccessDeniedException') {
      throw new Error('Insufficient permissions to read CloudWatch Logs');
   }

   if (errorName === 'ServiceUnavailableException') {
      throw new Error('CloudWatch Logs service unavailable, try again later');
   }

   const isCredError = errorName === 'CredentialsProviderError'
      || errorName === 'CredentialsError'
      || errorMessage.includes('credentials');

   if (isCredError) {
      throw new Error('AWS credentials not configured or invalid');
   }

   const isNetworkError = errorName === 'NetworkingError'
      || errorMessage.includes('ENOTFOUND')
      || errorMessage.includes('ECONNREFUSED')
      || errorMessage.includes('network');

   if (isNetworkError) {
      throw new Error('Network error - unable to connect to AWS');
   }

   throw new Error(errorMessage || 'Unknown error occurred');
}

async function fetchLogs(
   client: CloudWatchLogsClient,
   params: LogQueryParams
): Promise<LogQueryResult> {
   const input: FilterLogEventsCommandInput = {
      logGroupName: params.logGroupName,
      startTime: params.startTime,
   };

   if (params.endTime) {
      input.endTime = params.endTime;
   }

   if (params.filterPattern) {
      input.filterPattern = params.filterPattern;
   }

   if (params.nextToken) {
      input.nextToken = params.nextToken;
   }

   try {
      const command = new FilterLogEventsCommand(input),
            response: FilterLogEventsCommandOutput = await client.send(command);

      const events: LogEvent[] = (response.events || []).map((event: FilteredLogEvent) => {
         return {
            timestamp: event.timestamp || 0,
            message: event.message || '',
            logStreamName: event.logStreamName || '',
         };
      });

      return {
         events,
         nextToken: response.nextToken,
      };
   } catch(error: unknown) {
      handleAWSError(error, params.logGroupName);
   }
}

interface LiveTailOptions {
   client: CloudWatchLogsClient;
   functionName: string;
   filter?: string;
   pollInterval: number;
   log: (msg: string) => void;
   exit: () => void;
}

async function startLiveTail(opts: LiveTailOptions): Promise<void> {
   process.on('SIGINT', () => {
      opts.log('\nStopping live tail...');
      opts.exit();
   });

   let lastSeenTimestamp = Date.now() - (1 * 60 * 1000);

   const initialParams = buildQueryParams({
      functionName: opts.functionName,
      minutes: 1,
      filter: opts.filter,
   });

   const initialResult = await fetchLogs(opts.client, initialParams);

   initialResult.events.forEach((event: LogEvent) => {
      opts.log(formatLogEvent(event));

      if (event.timestamp > lastSeenTimestamp) {
         lastSeenTimestamp = event.timestamp;
      }
   });

   const pollForNewLogs = async (): Promise<void> => {
      const now = Date.now(),
            minutesSinceLastSeen = Math.ceil((now - lastSeenTimestamp) / (60 * 1000));

      const params = buildQueryParams({
         functionName: opts.functionName,
         minutes: minutesSinceLastSeen,
         filter: opts.filter,
      });

      const result = await fetchLogs(opts.client, params);

      const newEvents = result.events.filter((event: LogEvent) => {
         return event.timestamp > lastSeenTimestamp;
      });

      newEvents.forEach((event: LogEvent) => {
         opts.log(formatLogEvent(event));

         if (event.timestamp > lastSeenTimestamp) {
            lastSeenTimestamp = event.timestamp;
         }
      });
   };

   // Intentionally never resolves; process exits via SIGINT handler
   return new Promise<void>((_resolve, reject) => {
      setInterval(() => {
         pollForNewLogs().catch(reject);
      }, opts.pollInterval);
   });
}

export default class Logs extends BaseCommand {

   public static summary = 'Retrieve and display Lambda function logs from CloudWatch';

   public static flags = {
      name: Flags.string({
         description: 'name of the Lambda function',
         required: true,
      }),
      minutes: Flags.integer({
         description: 'how many minutes back to query',
         default: 10,
         min: 1,
      }),
      filter: Flags.string({
         description: 'text pattern to filter log messages',
      }),
      live: Flags.boolean({
         description: 'enable live tail mode',
         default: false,
      }),
   };

   public async run(): Promise<void> {
      const { flags } = await this.parse(Logs);

      const client = new CloudWatchLogsClient({ region: flags.region });

      if (flags.live) {
         await startLiveTail({
            client,
            functionName: flags.name,
            filter: flags.filter,
            pollInterval: 2000,
            log: this.log.bind(this),
            exit: () => { this.exit(0); },
         });

         return;
      }

      const queryParams = buildQueryParams({
         functionName: flags.name,
         minutes: flags.minutes,
         filter: flags.filter,
      });

      const result = await fetchLogs(client, queryParams);

      if (result.events.length === 0) {
         if (flags.filter) {
            this.log(`No logs matching filter '${flags.filter}' found`);
         } else {
            this.log(`No logs found for function '${flags.name}' in the past ${flags.minutes} minutes`);
         }

         return;
      }

      result.events.forEach((event: LogEvent) => {
         this.log(formatLogEvent(event));
      });
   }

}
