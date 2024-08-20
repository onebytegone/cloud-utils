import { Message as SQSMessage } from '@aws-sdk/client-sqs';
import { isString, isStringMap, isStringUnknownMap, isUndefined } from '@silvermine/toolbox';

function isStringOrUndefined(o: unknown): o is string | undefined {
   return isString(o) || isUndefined(o);
}

export function isSQSMessage(o: unknown): o is SQSMessage {
   return isStringUnknownMap(o)
      && isStringOrUndefined(o.MessageId)
      && isStringOrUndefined(o.MessageId)
      && isStringOrUndefined(o.ReceiptHandle)
      && isStringOrUndefined(o.MD5OfBody)
      && isStringOrUndefined(o.Body)
      && (isStringMap(o.Attributes) || isUndefined(o.Attributes))
      && isStringOrUndefined(o.MD5OfMessageAttributes)
      && (isStringUnknownMap(o.MessageAttributes) || isUndefined(o.MessageAttributes));
}
