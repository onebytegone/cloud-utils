import { InvocationType, Lambda } from '@aws-sdk/client-lambda';
import { Flags } from '@oclif/core';
import { invokeLambdaFunction } from '../../lib/aws/invoke-lambda-function.js';
import { BaseCommand } from '../../base-command.js';

export default class Invoke extends BaseCommand {

   public static summary = 'Invoke a Lambda function with the provided payload';

   public static flags = {
      name: Flags.string({
         description: 'name of the Lambda function',
         required: true,
      }),
      payload: Flags.string({
         description: 'JSON payload to send to the function',
      }),
      'json-decode': Flags.boolean({
         description: 'attempt to JSON decode the response payload',
         default: true,
         allowNo: true,
      }),
      'invocation-type': Flags.custom<InvocationType>({
         description: 'invoke the function synchronously or asynchronously',
         options: Object.values(InvocationType),
         default: InvocationType.RequestResponse,
      })(),
   };

   public async run(): Promise<void> {
      const { flags } = await this.parse(Invoke),
            lambda = new Lambda({ region: flags.region });

      const resp = await invokeLambdaFunction(lambda, {
         name: flags.name,
         payload: flags.payload,
         invocationType: flags['invocation-type'],
      });

      let responsePayload = resp.responsePayload;

      if (flags['json-decode'] && responsePayload) {
         try {
            responsePayload = JSON.parse(responsePayload);
         } catch(_e) {
            // noop
         }
      }

      if (resp.error) {
         this.logToStderr(JSON.stringify({
            error: resp.error,
            responsePayload,
         }));
         this.exit(1);
      }

      this.log(JSON.stringify({
         responsePayload,
      }));
   }

}
