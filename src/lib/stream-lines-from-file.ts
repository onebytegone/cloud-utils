import { createReadStream } from 'fs';

// Node's readline splits on U+2028 and U+2029, which are valid raw characters
// inside JSON string values. Splitting only on LF/CRLF keeps such records intact.
export async function* streamLinesFromFile(file: string): AsyncIterable<string> {
   const stream = createReadStream(file, { encoding: 'utf8' });

   let buffer = '';

   for await (const chunk of stream) {
      buffer += chunk;

      let newlineIndex = buffer.indexOf('\n');

      while (newlineIndex >= 0) {
         const line = buffer.slice(0, newlineIndex).replace(/\r$/, '');

         buffer = buffer.slice(newlineIndex + 1);
         yield line; // eslint-disable-line no-restricted-syntax
         newlineIndex = buffer.indexOf('\n');
      }
   }

   const lastLine = buffer.replace(/\r$/, '');

   if (lastLine !== '') {
      yield lastLine; // eslint-disable-line no-restricted-syntax
   }
}
