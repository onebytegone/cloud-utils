import { Command, Option } from 'commander';
import PQueue from 'p-queue';
import chalk from 'chalk';
import { streamLinesFromFile } from '../../lib/stream-lines-from-file';
import { RestoreObjectCommand, S3Client, Tier } from '@aws-sdk/client-s3';
import createWriteStream from '../../lib/create-write-stream';
import { generateDefaultOutputFilenameTimestamp } from '../../lib/generate-default-output-filename';

interface CommandOptions {
   name: string;
   inputFile: string;
   successful: string;
   restoreDays: string;
   concurrency: number;
}

const s3 = new S3Client({});

async function bulkInvokeLambdaFunction(this: Command, opts: CommandOptions): Promise<void> {
   const queue = new PQueue({ concurrency: opts.concurrency }),
         maxQueueSize = opts.concurrency * 5,
         successfulRestoreRequestsFile = opts.successful || `s3-bulk-restore-successful-${generateDefaultOutputFilenameTimestamp()}.txt`,
         successfulRestoreRequestsWriteStream = await createWriteStream(successfulRestoreRequestsFile),
         counters = { processed: 0 };

   console.info(chalk.yellow(
      `Starting bulk restore of files listed in ${opts.inputFile} (concurrency: ${opts.concurrency})`
   ));

   for await (const s3URI of streamLinesFromFile(opts.inputFile)) {
      queue.add(async () => {
         const matches = s3URI.match(/^s3:\/\/([^/]+)\/(.*)/);

         if (!matches) {
            console.error(chalk.red(`Invalid S3 URI: ${s3URI}`));
            return;
         }

         try {
            await s3.send(new RestoreObjectCommand({
               Bucket: matches[1],
               Key: matches[2],
               RestoreRequest: {
                  Days: Number(opts.restoreDays),
                  GlacierJobParameters: {
                     Tier: Tier.Bulk,
                  },
               },
            }));
         } catch(e) {
            if (e.name === 'RestoreAlreadyInProgress') {
               console.info(chalk.gray(`Restore is already in progress for ${s3URI}`));
               return;
            }

            throw e;
         }

         successfulRestoreRequestsWriteStream.write(s3URI + '\n');
         counters.processed += 1;

         if (counters.processed % 10 === 0) {
            console.info(chalk.gray(`Status: ${counters.processed} objects processed`));
         }
      });

      if (queue.size > maxQueueSize) {
         await queue.onEmpty();
      }
   }

   await queue.onIdle();

   console.info(chalk.whiteBright(
      `Total: ${counters.processed} objects processed`
   ));

   successfulRestoreRequestsWriteStream.close();
}

export default function register(command: Command): void {
   /* eslint-disable @silvermine/silvermine/call-indentation */
   command
      .description('Initiates Glacier restores for the provided S3 URIs')
      .requiredOption('-i, --input-file <path>', 'name of the file containing newline-delimited S3 URIs to restore')
      .option('--successful <path>', 'name of the file to write the response from successful restores')
      .addOption(
         new Option('--restore-days <days>', 'number of days to keep the restored files')
            .argParser((value) => {
               return Number(value);
            })
            .default(3)
      )
      .addOption(
         new Option('--concurrency <number>', 'number of concurrent invocation requests')
            .argParser((value) => {
               return Number(value);
            })
            .default(10)
      )
      .action(bulkInvokeLambdaFunction);
   /* eslint-enable @silvermine/silvermine/call-indentation */
}
