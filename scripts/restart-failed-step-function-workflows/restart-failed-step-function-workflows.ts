import {
   SFNClient,
   ExecutionStatus,
   ExecutionListItem,
   GetExecutionHistoryCommand,
   StartExecutionCommand,
} from '@aws-sdk/client-sfn';
import minimist from 'minimist';
import PQueue from 'p-queue';
import { getRequiredArg } from '../../lib/minimist/get-required-arg';
import { IterateOverStepFunctionExecutionsInput, iterateOverStepFunctionExecutions } from '../../lib/aws/iterate-step-function-executions';

const sf = new SFNClient({}),
      limiter = new PQueue({ concurrency: 100 }),
      argv = minimist(process.argv, { string: [ 'arn', 'min-start-date' ], boolean: [ 'dry-run' ] }),
      stateMachineARN = getRequiredArg(argv, 'arn') as string;

(async () => {
   const input: IterateOverStepFunctionExecutionsInput = {
      stateMachineARN,
      statusFilter: ExecutionStatus.FAILED,
      minStartDate: argv['min-start-date'] ? new Date(argv['min-start-date']) : undefined,
   };

   await iterateOverStepFunctionExecutions(input, (execution: ExecutionListItem): Promise<void> => {
      return limiter.add(async () => {
         const executionHistory = await sf.send(new GetExecutionHistoryCommand({
            executionArn: execution.executionArn,
            maxResults: 1,
         }));

         if (!executionHistory.events) {
            console.warn(`No event history was found for ${execution.executionArn}. Not restarting.`);
            return;
         }

         if (executionHistory.events[0].executionStartedEventDetails?.inputDetails?.truncated) {
            console.warn(`Returned input for ${execution.executionArn} is truncated. Not restarting.`);
            return;
         }

         const input = executionHistory.events[0].executionStartedEventDetails?.input;

         if (!input) {
            console.warn(`No input was returned for ${execution.executionArn}. Not restarting.`);
            return;
         }

         if (argv['dry-run']) {
            console.info(`(dry-run) Would restart ${execution.executionArn} from ${execution.startDate?.toISOString()}`, JSON.stringify(input));
         } else {
            const restarted = await sf.send(new StartExecutionCommand({
               stateMachineArn: stateMachineARN,
               input,
            }));

            console.info(`Restarted ${execution.executionArn} (${execution.startDate?.toISOString()}) as ${restarted.executionArn}`, JSON.stringify(input));
         }
      });
   });
})();
