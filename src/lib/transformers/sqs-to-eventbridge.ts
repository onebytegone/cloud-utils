import { EventBridgeEvent } from 'aws-lambda';
import { Message as SQSMessage } from '@aws-sdk/client-sqs';
import { isSQSMessage } from '../aws/typeguards/is-sqs-message';
import { isString } from '@silvermine/toolbox';
import { isEventBridgeEvent } from '../aws/typeguards/is-eventbridge-event';

export function extractEventBridgeEventFromSQSMessage(input: SQSMessage): EventBridgeEvent<string, unknown>;
export function extractEventBridgeEventFromSQSMessage(input: unknown): EventBridgeEvent<string, unknown> {
   if (!isSQSMessage(input)) {
      throw new Error(`Provided input is not an SQS message, received: ${JSON.stringify(input)}`);
   }

   if (!isString(input.Body)) {
      throw new Error(`Provided input is an SQS message, but it has no body. Message: ${JSON.stringify(input)}`);
   }

   let parsedBody: unknown;

   try {
      parsedBody = JSON.parse(input.Body);
   } catch(e) {
      throw new Error(`Body of the provided SQS message failed to parse. Error: ${e}; Message: ${JSON.stringify(input)}`);
   }

   if (!isEventBridgeEvent(parsedBody)) {
      throw new Error(`Body of the provided SQS message is not an EventBridgeEvent. Message: ${JSON.stringify(input)}`);
   }

   return parsedBody;
}
