import { Writable } from 'stream';
import { AttributeValue } from '@aws-sdk/client-dynamodb';
import { unmarshall } from '@aws-sdk/util-dynamodb';
import writeWithBackpressure from '../write-with-backpressure.js';

// JSON.stringify renders Uint8Array as {"0":n,...} and Set as {}. Encode binary
// values as base64 and unwrap sets so NDJSON consumers get meaningful values.
function ndjsonReplacer(_key: string, value: unknown): unknown {
   if (value instanceof Uint8Array) {
      return Buffer.from(value).toString('base64');
   }
   if (value instanceof Set) {
      return Array.from(value);
   }
   return value;
}

export default async function emitItemNDJSON(
   sink: Writable,
   item: Record<string, AttributeValue>
): Promise<void> {
   const line = JSON.stringify(unmarshall(item), ndjsonReplacer) + '\n';

   await writeWithBackpressure(sink, line);
}
