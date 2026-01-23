import { Command, Option } from 'commander';
import {
   CloudWatchLogsClient,
   FilterLogEventsCommand,
   FilterLogEventsCommandInput,
   FilterLogEventsCommandOutput,
} from '@aws-sdk/client-cloudwatch-logs';
import chalk from 'chalk';
import { quitWithError } from '../../lib/quit-with-error';

interface CommandOptions {
   name: string;
   minutes: number;
   filter?: string;
   live: boolean;
}

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

function validateMinutes(minutes: number): void {
   if (typeof minutes !== 'number' || isNaN(minutes)) {
      quitWithError('Error: minutes must be a valid number');
   }

   if (minutes < 1) {
      quitWithError('Error: minutes must be at least 1');
   }
}

function validateFunctionName(name: string): void {
   if (!name || name.trim() === '') {
      quitWithError('Error: function name cannot be empty');
   }
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

      const events: LogEvent[] = (response.events || []).map((event: any) => {
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
   } catch(error: any) {
      handleAWSError(error, params.logGroupName);
   }
}

function handleAWSError(error: any, logGroupName?: string): never {
   const errorName = error.name || '',
         errorMessage = error.message || '';

   if (errorName === 'ResourceNotFoundException') {
      const groupInfo = logGroupName ? ` (${logGroupName})` : '';

      quitWithError(`Error: Function not found or has no log group${groupInfo}`);
   }

   if (errorName === 'AccessDeniedException') {
      quitWithError('Error: Insufficient permissions to read CloudWatch Logs');
   }

   if (errorName === 'ServiceUnavailableException') {
      quitWithError('Error: CloudWatch Logs service unavailable, try again later');
   }

   const isCredError = errorName === 'CredentialsProviderError'
      || errorName === 'CredentialsError'
      || errorMessage.includes('credentials');

   if (isCredError) {
      quitWithError('Error: AWS credentials not configured or invalid');
   }

   const isNetworkError = errorName === 'NetworkingError'
      || errorMessage.includes('ENOTFOUND')
      || errorMessage.includes('ECONNREFUSED')
      || errorMessage.includes('network');

   if (isNetworkError) {
      quitWithError('Error: Network error - unable to connect to AWS');
   }

   quitWithError(`Error: ${errorMessage || 'Unknown error occurred'}`);
}

interface LiveTailOptions {
   client: CloudWatchLogsClient;
   functionName: string;
   filter?: string;
   pollInterval: number;
}

async function startLiveTail(opts: LiveTailOptions): Promise<void> {
   process.on('SIGINT', () => {
      console.info('\nStopping live tail...');
      process.exit(0);
   });

   let lastSeenTimestamp = Date.now() - (1 * 60 * 1000);

   const initialParams = buildQueryParams({
      functionName: opts.functionName,
      minutes: 1,
      filter: opts.filter,
   });

   const initialResult = await fetchLogs(opts.client, initialParams);

   initialResult.events.forEach((event: LogEvent) => {
      console.info(formatLogEvent(event));

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
         console.info(formatLogEvent(event));

         if (event.timestamp > lastSeenTimestamp) {
            lastSeenTimestamp = event.timestamp;
         }
      });
   };

   setInterval(pollForNewLogs, opts.pollInterval);
}

async function getLambdaLogs(this: Command, opts: CommandOptions): Promise<void> {
   validateFunctionName(opts.name);
   validateMinutes(opts.minutes);

   const client = new CloudWatchLogsClient({});

   if (opts.live) {
      await startLiveTail({
         client,
         functionName: opts.name,
         filter: opts.filter,
         pollInterval: 2000,
      });

      return;
   }

   const queryParams = buildQueryParams({
      functionName: opts.name,
      minutes: opts.minutes,
      filter: opts.filter,
   });

   const result = await fetchLogs(client, queryParams);

   if (result.events.length === 0) {
      if (opts.filter) {
         console.info(`No logs matching filter '${opts.filter}' found`);
      } else {
         console.info(`No logs found for function '${opts.name}' in the past ${opts.minutes} minutes`);
      }

      return;
   }

   result.events.forEach((event: LogEvent) => {
      console.info(formatLogEvent(event));
   });
}

export default function register(command: Command): void {
   /* eslint-disable @silvermine/silvermine/call-indentation */
   command
      .description(
         'Retrieves and displays AWS Lambda function logs from CloudWatch Logs'
      )
      .requiredOption('--name <string>', 'name of the Lambda function')
      .addOption(
         new Option('--minutes <number>', 'how many minutes back to query')
            .argParser((value: string) => {
               return Number(value);
            })
            .default(10)
      )
      .option('--filter <string>', 'text pattern to filter log messages')
      .option('--live', 'enable live tail mode', false)
      .action(getLambdaLogs);
   /* eslint-enable @silvermine/silvermine/call-indentation */
}
