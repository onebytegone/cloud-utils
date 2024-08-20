import { createWriteStream as fsCreateWriteStream, WriteStream } from 'fs';
import { mkdir } from 'fs/promises';
import { dirname } from 'path';

export default async function createWriteStream(filePath: string): Promise<WriteStream> {
   await mkdir(dirname(filePath), { recursive: true });

   return fsCreateWriteStream(filePath, 'utf-8');
}
