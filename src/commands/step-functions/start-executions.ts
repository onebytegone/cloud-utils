import { Command, Option } from 'commander';
import { SFNClient, StartExecutionCommand } from '@aws-sdk/client-sfn';
import { getStateMachineARN } from '../../lib/aws/get-state-machine-arn';
import { quitWithError } from '../../lib/quit-with-error';
import PQueue from 'p-queue';
import { streamLinesFromFile } from '../../lib/stream-lines-from-file';
import { v4 as uuidv4 } from 'uuid';

const sfn = new SFNClient({}),
      NAMED_INPUT_PATTERN = /([0-9A-Za-z-_]{1,80})[\t ]({.*})$/;

interface CommandOptions {
   name: string;
   inputsFile: string;
   concurrency: number;
   appendRandomSuffix: boolean;
}

function parseInputLine(inputLine: string, appendRandomSuffix: boolean): { name?: string; input: string } {
   const matches = inputLine.match(NAMED_INPUT_PATTERN);

   if (matches) {
      return {
         name: matches[1] + (appendRandomSuffix ? uuidv4().slice(0, 8) : ''),
         input: matches[2],
      };
   }

   // input isn't prefixed with an execution name
   return { input: inputLine };
}

async function startStepFunctionsWorkflowExecutions(this: Command, opts: CommandOptions): Promise<void> {
   const queue = new PQueue({ concurrency: opts.concurrency }),
         maxQueueSize = opts.concurrency * 5,
         stateMachineArn = await getStateMachineARN(sfn, opts.name);

   if (!stateMachineArn) {
      quitWithError(`Could not find a state machine with the name "${opts.name}"`);
   }

   for await (const inputLine of streamLinesFromFile(opts.inputsFile)) {
      queue.add(async () => {
         const { name, input } = parseInputLine(inputLine, opts.appendRandomSuffix),
               resp = await sfn.send(new StartExecutionCommand({ stateMachineArn, input, name }));

         console.info(JSON.stringify({
            input,
            execution: resp.executionArn,
         }));
      });

      if (queue.size > maxQueueSize) {
         await queue.onEmpty();
      }
   }

   await queue.onIdle();
}

export default function register(command: Command): void {
   /* eslint-disable @silvermine/silvermine/call-indentation */
   command
      .description(
         'Starts Step Functions workflow executions with the provided inputs'
      )
      .requiredOption('--name <string>', 'name of the Step Functions workflow')
      .requiredOption(
         '-i, --inputs-file <path>',
         'name of the file containing newline-delimited inputs, optionally prefixed with an execution name'
      )
      .addOption(
         new Option('--concurrency <number>', 'number of concurrent invocation requests')
            .argParser((value) => {
               return Number(value);
            })
            .default(10)
      )
      .option('--no-append-random-suffix', 'don\'t append an 8 character random suffix to provided execution names')
      .action(startStepFunctionsWorkflowExecutions);
   /* eslint-enable @silvermine/silvermine/call-indentation */
}
