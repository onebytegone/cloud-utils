import { createWriteStream as fsCreateWriteStream, WriteStream } from 'fs';
import { mkdir } from 'fs/promises';
import { dirname } from 'path';
import { quitWithError } from './quit-with-error';

export default async function createWriteStream(filePath: string): Promise<WriteStream> {
   await mkdir(dirname(filePath), { recursive: true });

   const stream = fsCreateWriteStream(filePath, 'utf-8');

   stream.on('error', (err) => {
      quitWithError(`Error writing to ${filePath}: ${err.message}`);
   });

   return stream;
}

export function endWriteStream(stream: WriteStream): Promise<void> {
   return new Promise((resolve, reject) => {
      stream.once('finish', () => {
         resolve();
      });
      stream.once('error', reject);
      stream.end();
   });
}
