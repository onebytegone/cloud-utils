import { once } from 'events';
import { Writable } from 'stream';

export default async function writeWithBackpressure(sink: Writable, line: string): Promise<void> {
   if (!sink.write(line)) {
      await once(sink, 'drain');
   }
}
