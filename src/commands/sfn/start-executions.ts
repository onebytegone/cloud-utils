import { SFNClient, StartExecutionCommand } from '@aws-sdk/client-sfn';
import PQueue from 'p-queue';
import chalk from 'chalk';
import { Flags } from '@oclif/core';
import { v4 as uuidv4 } from 'uuid';
import { getStateMachineARN } from '../../lib/aws/get-state-machine-arn.js';
import { streamLinesFromFile } from '../../lib/stream-lines-from-file.js';
import { generateDefaultOutputFilename } from '../../lib/generate-default-output-filename.js';
import createWriteStream from '../../lib/create-write-stream.js';
import endWriteStream from '../../lib/end-write-stream.js';
import { BaseCommand } from '../../base-command.js';

const NAMED_INPUT_PATTERN = /([0-9A-Za-z-_]{1,80})[\t ]({.*})$/;

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

export default class StartExecutions extends BaseCommand {

   public static summary = 'Start Step Functions workflow executions with the provided inputs';

   public static flags = {
      name: Flags.string({
         description: 'name of the Step Functions workflow',
         required: true,
      }),
      'inputs-file': Flags.string({
         char: 'i',
         description: 'name of the file containing newline-delimited inputs, optionally prefixed with an execution name',
         required: true,
      }),
      failed: Flags.string({
         description: 'name of the file to write information about failed executions',
      }),
      concurrency: Flags.integer({
         description: 'number of concurrent invocation requests',
         default: 10,
      }),
      'append-random-suffix': Flags.boolean({
         description: 'append an 8 character random suffix to provided execution names',
         default: true,
         allowNo: true,
      }),
   };

   public async run(): Promise<void> {
      const { flags } = await this.parse(StartExecutions),
            sfn = new SFNClient({ region: flags.region }),
            queue = new PQueue({ concurrency: flags.concurrency }),
            maxQueueSize = flags.concurrency * 5,
            stateMachineArn = await getStateMachineARN(sfn, flags.name),
            failedOutputFile = flags.failed || generateDefaultOutputFilename('sfn', 'start-executions', flags.name, 'failed');

      if (!stateMachineArn) {
         this.error(`Could not find a state machine with the name "${flags.name}"`);
      }

      const failedWriteStream = await createWriteStream(failedOutputFile),
            counters = { started: 0, failed: 0 };

      this.logInfoToStderr(`${chalk.gray('Failed output:')} ${failedOutputFile}`);

      for await (const inputLine of streamLinesFromFile(flags['inputs-file'])) {
         queue.add(async () => {
            const { name, input } = parseInputLine(inputLine, flags['append-random-suffix']);

            try {
               const resp = await sfn.send(new StartExecutionCommand({ stateMachineArn, input, name }));

               counters.started += 1;
               this.log(JSON.stringify({
                  input,
                  execution: resp.executionArn,
               }));
            } catch(e) {
               const message = e instanceof Error ? e.message : String(e);

               counters.failed += 1;
               this.logToStderr(chalk.red(`Failed to start execution (input: ${input}): ${message}`));
               failedWriteStream.write(JSON.stringify({ input, name, error: message }) + '\n');
            }
         });

         if (queue.size > maxQueueSize) {
            await queue.onEmpty();
         }
      }

      await queue.onIdle();

      await endWriteStream(failedWriteStream);

      this.logInfoToStderr(chalk.whiteBright(
         `Total: ${counters.started + counters.failed} executions (${counters.started} started / ${counters.failed} failed)`
      ));

      if (counters.failed > 0) {
         this.exit(1);
      }
   }

}
