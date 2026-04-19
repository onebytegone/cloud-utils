import { createWriteStream as fsCreateWriteStream, WriteStream } from 'fs';
import { mkdir } from 'fs/promises';
import { dirname } from 'path';

export default async function createWriteStream(
   filePath: string,
   opts: { append?: boolean } = {}
): Promise<WriteStream> {
   await mkdir(dirname(filePath), { recursive: true });

   return fsCreateWriteStream(filePath, {
      encoding: 'utf-8',
      flags: opts.append ? 'a' : 'w',
   });
}
