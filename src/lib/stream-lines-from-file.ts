import { createReadStream } from 'fs';
import { createInterface } from 'readline';

export async function* streamLinesFromFile(file: string): AsyncIterable<string> {
   const rl = createInterface({
      input: createReadStream(file),
      crlfDelay: Infinity,
   });

   for await (const line of rl) {
      yield line; // eslint-disable-line no-restricted-syntax
   }
}
