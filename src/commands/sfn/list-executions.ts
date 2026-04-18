import {
   ExecutionStatus,
   GetExecutionHistoryCommandInput,
   HistoryEvent,
   ListExecutionsCommandInput,
   paginateGetExecutionHistory,
   paginateListExecutions,
   SFNClient,
} from '@aws-sdk/client-sfn';
import { Flags } from '@oclif/core';
import { DateTime } from 'luxon';
import { getStateMachineARN } from '../../lib/aws/get-state-machine-arn.js';
import { generateDefaultOutputFilename } from '../../lib/generate-default-output-filename.js';
import createWriteStream from '../../lib/create-write-stream.js';
import { BaseCommand } from '../../base-command.js';

export default class ListExecutions extends BaseCommand {

   public static summary = 'List executions for a state machine';

   public static flags = {
      name: Flags.string({
         description: 'name of the state machine',
         required: true,
      }),
      output: Flags.string({
         char: 'o',
         description: 'name of the file to write the executions',
      }),
      status: Flags.custom<ExecutionStatus>({
         char: 's',
         description: 'state machine status to filter to',
         options: Object.values(ExecutionStatus),
      })(),
      'started-after': Flags.string({
         description: 'output all executions that started after the provided ISO 8601 date time',
         parse: async (value: string): Promise<string> => {
            const dt = DateTime.fromISO(value);

            if (!dt.isValid) {
               throw new Error(`Invalid ISO 8601 date: ${value}`);
            }

            return value;
         },
      }),
      'max-executions': Flags.integer({
         char: 'n',
         description: 'maximum number of executions to fetch',
      }),
      history: Flags.option({
         char: 'H',
         description: 'include execution events; to limit events see --max-events',
         options: [ 'head', 'reverse' ] as const,
      })(),
      'max-events': Flags.integer({
         description: 'maximum number of events to fetch for each execution',
      }),
   };

   // eslint-disable-next-line complexity
   public async run(): Promise<void> {
      const { flags } = await this.parse(ListExecutions);

      const history = (flags['max-events'] && !flags.history) ? 'head' : flags.history,
            sfn = new SFNClient({ region: flags.region, maxAttempts: 20 }),
            startedAfter = flags['started-after'] ? DateTime.fromISO(flags['started-after']) : undefined;

      const stateMachineArn = await getStateMachineARN(sfn, flags.name);

      if (!stateMachineArn) {
         this.error(`Could not find a state machine with the name "${flags.name}"`);
      }

      const writeStream = await createWriteStream(flags.output || generateDefaultOutputFilename('sfn', 'list-executions', flags.name));

      const listParams: ListExecutionsCommandInput = {
         stateMachineArn,
         statusFilter: flags.status,
         maxResults: flags['max-executions'] ? Math.min(flags['max-executions'], 1000) : undefined,
      };

      let foundExecutions = 0;

      for await (const listResp of paginateListExecutions({ client: sfn }, listParams)) {
         for (const execution of listResp.executions || []) {
            if (startedAfter && execution.startDate && execution.startDate < startedAfter.toJSDate()) {
               writeStream.end();
               return;
            }

            const events: HistoryEvent[] = [];

            if (history) {
               const historyParams: GetExecutionHistoryCommandInput = {
                  executionArn: execution.executionArn,
                  reverseOrder: history === 'reverse',
                  maxResults: flags['max-events'] ? Math.min(flags['max-events'], 1000) : undefined,
               };

               for await (const historyResp of paginateGetExecutionHistory({ client: sfn }, historyParams)) {
                  events.push(...(historyResp.events || []));

                  // eslint-disable-next-line max-depth
                  if (flags['max-events'] && events.length >= flags['max-events']) {
                     // TODO: should events be trimmed to flags['max-events']?
                     break;
                  }
               }
            }

            foundExecutions += 1;
            writeStream.write(JSON.stringify({
               ...execution,
               events: history ? events : undefined,
            }) + '\n');

            if (flags['max-executions'] && foundExecutions >= flags['max-executions']) {
               writeStream.end();
               return;
            }
         }
      }

      writeStream.end();
   }

}
