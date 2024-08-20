import { InvocationType, InvokeCommand, LambdaClient } from '@aws-sdk/client-lambda';

export interface InvokeFunctionParams {
   name: string;
   invocationType: InvocationType;
   payload?: string;
}

export interface InvokeFunctionOutput {
   statusCode?: number;
   error?: string;
   responsePayload?: string;
}

export async function invokeLambdaFunction(client: LambdaClient, params: InvokeFunctionParams): Promise<InvokeFunctionOutput> {
   const resp = await client.send(new InvokeCommand({
      FunctionName: params.name,
      InvocationType: params.invocationType,
      Payload: params.payload,
   }));

   return {
      statusCode: resp.StatusCode,
      error: resp.FunctionError,
      responsePayload: resp.Payload ? Buffer.from(resp.Payload).toString() : undefined,
   };
}
