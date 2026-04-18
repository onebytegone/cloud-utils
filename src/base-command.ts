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
   };

}
