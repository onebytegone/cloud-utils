import { Command, Flags } from '@oclif/core';

export abstract class BaseCommand extends Command {

   // Required by oclif convention
   // eslint-disable-next-line @typescript-eslint/naming-convention
   public static baseFlags = {
      region: Flags.string({
         description: 'AWS region to use',
         helpGroup: 'GLOBAL',
         env: 'AWS_REGION',
      }),
      silent: Flags.boolean({
         description: 'suppress informational messages; only errors and data are emitted',
         helpGroup: 'GLOBAL',
         default: false,
      }),
   };

   protected logInfoToStderr(message: string): void {
      if (!this.argv.includes('--silent')) {
         this.logToStderr(message);
      }
   }

}
