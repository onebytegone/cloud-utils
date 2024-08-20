import { isArrayOfStrings, isString, isStringUnknownMap, isUndefined } from '@silvermine/toolbox';
import { EventBridgeEvent } from 'aws-lambda';

export function isEventBridgeEvent(o: unknown): o is EventBridgeEvent<string, unknown> {
   return isStringUnknownMap(o)
      && isString(o.id)
      && isString(o.version)
      && isString(o.account)
      && isString(o.time)
      && isString(o.region)
      && isArrayOfStrings(o.resources)
      && isString(o.source)
      && isString(o['detail-type'])
      && isStringUnknownMap(o.detail)
      && (isString(o['replay-name']) || isUndefined(o['replay-name']));
}
