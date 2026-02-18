import { isArray, isEmpty } from '@silvermine/toolbox';

export async function* batchRecords<T>(iterable: AsyncIterable<T | T[]>, batchSize: number): AsyncIterable<T[]> {
   let batch: T[] = [];

   for await (const records of iterable) {
      for (const record of (isArray(records) ? records : [ records ])) {
         batch.push(record);

         if (batch.length === batchSize) {
            yield batch; // eslint-disable-line no-restricted-syntax
            batch = [];
         }
      }
   }

   if (!isEmpty(batch)) {
      yield batch; // eslint-disable-line no-restricted-syntax
   }
}
