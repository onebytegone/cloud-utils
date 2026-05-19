import { createInterface, Interface } from 'readline';

function cleanup(rl: Interface): void {
   rl.removeAllListeners('line');
   rl.removeAllListeners('close');
   rl.close();
}

export default async function confirmBucketName(expected: string): Promise<boolean> {
   process.stderr.write(
      'This will permanently delete every object, version, delete marker,\n'
      + `and in-progress multipart upload in bucket "${expected}".\n`
      + 'Type the bucket name to confirm: '
   );

   const rl = createInterface({ input: process.stdin });

   return new Promise<boolean>((resolve) => {
      rl.once('line', (line: string): void => {
         cleanup(rl);
         resolve(line.trim() === expected);
      });

      rl.once('close', (): void => {
         cleanup(rl);
         resolve(false);
      });
   });
}
