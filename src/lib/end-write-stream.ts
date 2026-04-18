import { WriteStream } from 'fs';
import { finished } from 'stream/promises';

export default async function endWriteStream(stream: WriteStream): Promise<void> {
   stream.end();
   await finished(stream);
}
