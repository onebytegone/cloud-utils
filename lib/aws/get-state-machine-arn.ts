import { SFNClient, paginateListStateMachines } from '@aws-sdk/client-sfn';

export async function getStateMachineARN(client: SFNClient, name: string): Promise<string | undefined> {
   for await (const resp of paginateListStateMachines({ client }, {})) {
      const matchingStateMachine = resp.stateMachines?.find((stateMachine) => {
         return stateMachine.name === name;
      });

      if (matchingStateMachine) {
         return matchingStateMachine.stateMachineArn;
      }
   }
}
