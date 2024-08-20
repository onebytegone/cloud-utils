import { Command } from 'commander';
import createWriteStream from '../lib/create-write-stream';
import { streamLinesFromFile } from '../lib/stream-lines-from-file';
import { extractEventBridgeEventFromSQSMessage } from '../lib/transformers/sqs-to-eventbridge';
import { isUndefined } from '@silvermine/toolbox';

interface CommandOptions {
   input: string;
   output?: string;
   transformation: string;
}

const TRANSFORMATION_FUNCTIONS: Partial<Record<string, (input: any) => unknown>> = {
   'sqs-to-eventbridge-event': extractEventBridgeEventFromSQSMessage,
};

function getTransformationFn(name: string): (input: unknown) => unknown {
   const fn = TRANSFORMATION_FUNCTIONS[name];

   if (isUndefined(fn)) {
      console.error(
         `Could not find transformation named "${name}". Available transformations: ${Object.keys(TRANSFORMATION_FUNCTIONS).join(', ')}`
      );
      process.exit(1);
   }

   return fn;
}

async function createOutputWriter(outputFile?: string): Promise<{ write: (line: string) => void; close: () => void }> {
   if (outputFile) {
      const stream = await createWriteStream(outputFile);

      return {
         write: (line: string) => {
            stream.write(line + '\n');
         },
         close: () => {
            stream.close();
         },
      };
   }

   return {
      write: console.info,
      close: () => {}, // eslint-disable-line no-empty-function
   };
}

async function performTransformation(this: Command, opts: CommandOptions): Promise<void> {
   const outputWriter = await createOutputWriter(opts.output),
         transformationFn = getTransformationFn(opts.transformation);

   for await (const lines of streamLinesFromFile(opts.input)) {
      lines.forEach((line) => {
         outputWriter.write(JSON.stringify(transformationFn(JSON.parse(line))));
      });
   }

   outputWriter.close();
}

export default function register(command: Command): void {
   command
      .description('Transforms the provided input using the defined transformer')
      .requiredOption('-i, --input <path>', 'name of the file containing newline-delimited inputs')
      .requiredOption('-t, --transformation <name>', 'name of the transformation to apply')
      .option('-o, --output <path>', 'name of the file to write the transformed outputs')
      .action(performTransformation);
}
