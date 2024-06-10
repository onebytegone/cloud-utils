import { Command, Option } from 'commander';
import {
   ExecutionStatus,
   GetExecutionHistoryCommandInput,
   HistoryEvent,
   ListExecutionsCommandInput,
   paginateGetExecutionHistory,
   paginateListExecutions,
   SFNClient,
} from '@aws-sdk/client-sfn';
import { getStateMachineARN } from '../../../lib/aws/get-state-machine-arn';
import { quitWithError } from '../../../lib/quit-with-error';
import { DateTime } from 'luxon';
import { generateDefaultOutputFilename } from '../../../lib/generate-default-output-filename';
import createWriteStream from '../../../lib/create-write-stream';

const sfn = new SFNClient({});

interface CommandOptions {
   name: string;
   output?: string;
   status?: ExecutionStatus;
   startedAfter?: DateTime;
   maxExecutions?: number;
   history?: 'head' | 'reverse';
   maxEvents?: number;
}

async function listExecutions(this: Command, opts: CommandOptions): Promise<void> {
   const stateMachineArn = await getStateMachineARN(sfn, opts.name);

   if (!stateMachineArn) {
      quitWithError(`Could not find a state machine with the name "${opts.name}"`);
   }

   const writeStream = await createWriteStream(opts.output || generateDefaultOutputFilename('sfn', 'list-executions', opts.name));

   const listParams: ListExecutionsCommandInput = {
      stateMachineArn,
      statusFilter: opts.status,
      maxResults: opts.maxExecutions ? Math.min(opts.maxExecutions, 1000) : undefined,
   };

   let foundExecutions = 0;

   for await (const listResp of paginateListExecutions({ client: sfn }, listParams)) {
      for (const execution of listResp.executions || []) {
         if (opts.startedAfter && execution.startDate && execution.startDate < opts.startedAfter.toJSDate()) {
            writeStream.close();
            return;
         }

         const events: HistoryEvent[] = [];

         if (opts.history) {
            const historyParams: GetExecutionHistoryCommandInput = {
               executionArn: execution.executionArn,
               reverseOrder: opts.history === 'reverse',
               maxResults: opts.maxEvents ? Math.min(opts.maxEvents, 1000) : undefined,
            };

            for await (const historyResp of paginateGetExecutionHistory({ client: sfn }, historyParams)) {
               events.push(...(historyResp.events || []));

               // eslint-disable-next-line max-depth
               if (opts.maxEvents && events.length >= opts.maxEvents) {
                  // TODO: should events be trimmed to opts.maxEvents?
                  break;
               }
            }
         }

         foundExecutions += 1;
         writeStream.write(JSON.stringify({
            ...execution,
            events: opts.history ? events : undefined,
         }) + '\n');

         if (opts.maxExecutions && foundExecutions >= opts.maxExecutions) {
            writeStream.close();
            return;
         }
      }
   }

   writeStream.close();
}

export default function register(command: Command): void {
   /* eslint-disable @silvermine/silvermine/call-indentation */
   command
      .requiredOption('--name <string>', 'name of the state machine')
      .option('-o, --output <string>', 'name of the file to write the executions')
      .addOption(
         new Option('-s, --status <string>', 'state machine status to filter to')
            .choices(Object.values(ExecutionStatus))
      )
      .addOption(
         new Option('--started-after <iso8601>', 'Output all executions that started after the provided ISO 8601 date time')
            .argParser((value) => {
               return DateTime.fromISO(value);
            })
      )
      .addOption(
         new Option('-n, --max-executions <number>', 'Maximum number of executions to fetch')
            .argParser((value) => {
               return Number(value);
            })
      )
      .addOption(
         new Option('-h, --history [value]', 'Include all the execution\'s events. To limit the number of events, see \'--max-events <number>\'.') // eslint-disable-line max-len
            .choices([ 'head', 'reverse' ])
            .preset('head')
      )
      .addOption(
         new Option('--max-events <number>', 'Maximum number of events to fetch for each execution')
            .argParser((value) => {
               return Number(value);
            })
            .implies({ history: 'head' })
      )
      .action(listExecutions);
   /* eslint-enable @silvermine/silvermine/call-indentation */
}
