import { Command, Option } from 'commander';
import { InvocationType, Lambda } from '@aws-sdk/client-lambda';
import { invokeLambdaFunction } from '../../lib/aws/invoke-lambda-function';

interface CommandOptions {
   name: string;
   payload?: string;
   invocationType: InvocationType;
   jsonDecode: boolean;
   region?: string;
}

async function invokeLambdaFunctionCommand(this: Command, opts: CommandOptions): Promise<void> {
   const lambda = new Lambda({ region: opts.region });

   const resp = await invokeLambdaFunction(lambda, {
      name: opts.name,
      payload: opts.payload,
      invocationType: opts.invocationType,
   });

   let responsePayload = resp.responsePayload;

   if (opts.jsonDecode && responsePayload) {
      try {
         responsePayload = JSON.parse(responsePayload);
      } catch(e) {
         // noop
      }
   }

   if (resp.error) {
      console.error(JSON.stringify({
         error: resp.error,
         responsePayload,
      }));
      process.exit(1);
   }

   console.info(JSON.stringify({
      responsePayload,
   }));
}

export default function register(command: Command): void {
   /* eslint-disable @silvermine/silvermine/call-indentation */
   command
      .description(
         'Invokes a Lambda function with the provided payloads'
      )
      .requiredOption('--name <string>', 'name of the Lambda function')
      .option('--payload <string>', 'JSON payload to send to the function')
      .option('--no-json-decode', 'Don\'t attempt to JSON decode the function\'s response payload')
      .option('--region <value>', 'Region to send requests to')
      .addOption(
         new Option('--invocation-type <string>', 'invoke the function synchronously or asynchronously')
            .choices(Object.values(InvocationType))
            .default(InvocationType.RequestResponse)
      )
      .action(invokeLambdaFunctionCommand);
   /* eslint-enable @silvermine/silvermine/call-indentation */
}
