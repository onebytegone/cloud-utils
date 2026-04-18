import { Flags } from '@oclif/core';
import createWriteStream from '../lib/create-write-stream.js';
import { streamLinesFromFile } from '../lib/stream-lines-from-file.js';
import { extractEventBridgeEventFromSQSMessage } from '../lib/transformers/sqs-to-eventbridge.js';
import { isUndefined } from '@silvermine/toolbox';
import { BaseCommand } from '../base-command.js';

const TRANSFORMATION_FUNCTIONS: Partial<Record<string, (input: unknown) => unknown>> = {
   'sqs-to-eventbridge-event': extractEventBridgeEventFromSQSMessage,
};

function getTransformationFn(name: string): (input: unknown) => unknown {
   const fn = TRANSFORMATION_FUNCTIONS[name];

   if (isUndefined(fn)) {
      throw new Error(
         `Could not find transformation named "${name}". Available transformations: ${Object.keys(TRANSFORMATION_FUNCTIONS).join(', ')}`
      );
   }

   return fn;
}

async function createOutputWriter(log: (msg: string) => void, outputFile?: string): Promise<{ write: (line: string) => void; end: () => void }> {
   if (outputFile) {
      const stream = await createWriteStream(outputFile);

      return {
         write: (line: string) => {
            stream.write(line + '\n');
         },
         end: () => {
            stream.end();
         },
      };
   }

   return {
      write: log,
      end: () => {}, // eslint-disable-line no-empty-function
   };
}

export default class Transform extends BaseCommand {

   public static summary = 'Transform input using a defined transformer';

   public static flags = {
      input: Flags.string({
         char: 'i',
         description: 'name of the file containing newline-delimited inputs',
         required: true,
      }),
      transformation: Flags.string({
         char: 't',
         description: 'name of the transformation to apply',
         required: true,
      }),
      output: Flags.string({
         char: 'o',
         description: 'name of the file to write the transformed outputs',
      }),
   };

   public async run(): Promise<void> {
      const { flags } = await this.parse(Transform),
            outputWriter = await createOutputWriter(this.log.bind(this), flags.output),
            transformationFn = getTransformationFn(flags.transformation);

      for await (const line of streamLinesFromFile(flags.input)) {
         outputWriter.write(JSON.stringify(transformationFn(JSON.parse(line))));
      }

      outputWriter.end();
   }

}
