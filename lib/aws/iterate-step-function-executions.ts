import {
   ExecutionListItem,
   ExecutionStatus,
   ListExecutionsCommand,
   ListExecutionsCommandInput,
   SFNClient,
} from '@aws-sdk/client-sfn';

const sf = new SFNClient({});

export interface IterateOverStepFunctionExecutionsInput {
   stateMachineARN: string;
   statusFilter?: ExecutionStatus;
   minStartDate?: Date;
}

export function iterateOverStepFunctionExecutions(input: IterateOverStepFunctionExecutionsInput, callback: (execution: ExecutionListItem) => Promise<void>): Promise<void> {
   const params: ListExecutionsCommandInput = {
      stateMachineArn: input.stateMachineARN,
      statusFilter: input.statusFilter,
      maxResults: 1000,
   };

   return new Promise((resolve, reject) => {
      const loop = (): void => {
         sf.send(new ListExecutionsCommand(params))
            .then(async (resp) => {
               await Promise.all(
                  (resp.executions || [])
                     .filter((execution: ExecutionListItem): boolean => {
                        if (execution.startDate && input.minStartDate) {
                           return execution.startDate >= input.minStartDate;
                        }

                        return true;
                     })
                     .map(callback)
               );

               // TODO: Stop looping once past the given date?

               if (resp.nextToken) {
                  params.nextToken = resp.nextToken;
                  process.nextTick(loop);
               } else {
                  resolve(undefined);
               }
            })
            .catch(reject);
      };

      loop();
   });
}
