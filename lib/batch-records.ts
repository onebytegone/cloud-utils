import { isArray, isEmpty } from '@silvermine/toolbox';

export async function* batchRecords<T>(iteratable: AsyncIterable<T | T[]>, batchSize: number): AsyncIterable<T[]> {
   let batch = [];

   for await (const records of iteratable) {
      for (const record of isArray(records) ? records : [ records ]) {
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
