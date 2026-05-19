import {
   AbortMultipartUploadCommand,
   DeleteObjectsCommand,
   S3Client,
   S3ServiceException,
} from '@aws-sdk/client-s3';
import { Flags } from '@oclif/core';
import PQueue from 'p-queue';
import chalk from 'chalk';
import { BaseCommand } from '../../base-command.js';
import { batchRecords } from '../../lib/batch-records.js';
import toMessage from '../../lib/to-message.js';
import { listAllVersions } from '../../lib/s3/list-all-versions.js';
import { listMultipartUploads } from '../../lib/s3/list-multipart-uploads.js';
import confirmBucketName from '../../lib/s3/confirm-bucket-name.js';

const BATCH_SIZE = 1000,
      STATUS_INTERVAL = BATCH_SIZE * 10;

interface Counters {
   deleted: number;
   failed: number;
   abortedUploads: number;
   failedAborts: number;
}

interface BatchContext {
   counters: Counters;
   log: (msg: string) => void;
   warn: (msg: string) => void;
   print: (msg: string) => void;
}

function s3ErrorCode(e: unknown, fallback: string): string {
   return e instanceof S3ServiceException ? e.name : fallback;
}

async function sendDeleteBatch(
   client: S3Client,
   bucket: string,
   batch: { Key: string; VersionId: string }[],
   ctx: BatchContext
): Promise<void> {
   let response;

   try {
      response = await client.send(new DeleteObjectsCommand({
         Bucket: bucket,
         Delete: { Objects: batch, Quiet: true },
      }));
   } catch(e) {
      const message = toMessage(e),
            code = s3ErrorCode(e, 'BatchException');

      ctx.counters.failed += batch.length;
      ctx.warn(chalk.red(`Batch failed (${batch.length} objects): ${message}`));

      for (const obj of batch) {
         ctx.warn(JSON.stringify({ key: obj.Key, versionId: obj.VersionId, code, message }));
      }

      return;
   }

   const errors = response.Errors || [];

   ctx.counters.deleted += batch.length - errors.length;
   ctx.counters.failed += errors.length;

   const errorKeys = new Set(errors.map((e) => {
      return JSON.stringify([ e.Key, e.VersionId ]);
   }));

   for (const obj of batch) {
      if (errorKeys.has(JSON.stringify([ obj.Key, obj.VersionId ]))) {
         continue;
      }
      ctx.print(JSON.stringify({ key: obj.Key, versionId: obj.VersionId }));
   }

   for (const err of errors) {
      ctx.warn(JSON.stringify({
         key: err.Key || '',
         versionId: err.VersionId || '',
         code: err.Code || 'Unknown',
         message: err.Message || '',
      }));
   }

   const total = ctx.counters.deleted + ctx.counters.failed,
         prevTotal = total - batch.length;

   if (Math.floor(total / STATUS_INTERVAL) > Math.floor(prevTotal / STATUS_INTERVAL)) {
      ctx.log(chalk.gray(`Status: ${ctx.counters.deleted} deleted / ${ctx.counters.failed} failed`));
   }
}

async function abortUpload(
   client: S3Client,
   bucket: string,
   upload: { Key: string; UploadId: string },
   ctx: BatchContext
): Promise<void> {
   try {
      await client.send(new AbortMultipartUploadCommand({
         Bucket: bucket,
         Key: upload.Key,
         UploadId: upload.UploadId,
      }));
      ctx.counters.abortedUploads += 1;
      ctx.print(JSON.stringify({ key: upload.Key, uploadId: upload.UploadId }));
   } catch(e) {
      const message = toMessage(e),
            code = s3ErrorCode(e, 'AbortException');

      ctx.counters.failedAborts += 1;
      ctx.warn(chalk.red(`Failed to abort upload "${upload.Key}" [${upload.UploadId}]: ${message}`));
      ctx.warn(JSON.stringify({ key: upload.Key, uploadId: upload.UploadId, code, message }));
   }
}

export default class PurgeBucket extends BaseCommand {

   public static summary = 'Purge an S3 bucket: delete every object, version, delete marker, and abort in-progress multipart uploads';

   public static description = 'Empties an S3 bucket. The bucket itself is preserved. Includes all non-current '
      + 'versions and delete markers on versioned buckets, and aborts any in-progress multipart uploads '
      + '(which otherwise survive and incur storage cost). Destructive: requires an interactive bucket-name '
      + 'confirmation unless --yes is passed (or --dry-run, which never deletes).';

   public static flags = {
      bucket: Flags.string({
         description: 'name of the bucket to purge',
         required: true,
      }),
      prefix: Flags.string({
         description: 'only purge keys (and their versions/markers) starting with this prefix; also scopes the multipart-upload phase',
      }),
      'dry-run': Flags.boolean({
         description: 'list what would be deleted; issue no DeleteObjects or AbortMultipartUpload calls',
         default: false,
      }),
      yes: Flags.boolean({
         description: 'skip the interactive bucket-name confirmation prompt',
         default: false,
      }),
      concurrency: Flags.integer({
         description: 'max number of concurrent DeleteObjects / AbortMultipartUpload requests',
         default: 10,
         min: 1,
      }),
   };

   public async run(): Promise<void> {
      const { flags } = await this.parse(PurgeBucket),
            isDryRun = flags['dry-run'];

      if (!isDryRun && !flags.yes) {
         const confirmed = await confirmBucketName(flags.bucket);

         if (!confirmed) {
            this.error('Confirmation did not match. Aborting.', { exit: 1 });
         }
      }

      const client = new S3Client({ region: flags.region }),
            queue = new PQueue({ concurrency: flags.concurrency }),
            maxQueueSize = flags.concurrency * 5;

      const counters: Counters = {
         deleted: 0,
         failed: 0,
         abortedUploads: 0,
         failedAborts: 0,
      };

      const ctx: BatchContext = {
         counters,
         log: this.logInfoToStderr.bind(this),
         warn: this.logToStderr.bind(this),
         print: this.log.bind(this),
      };

      this.logInfoToStderr(chalk.yellow(
         `Starting purge of bucket "${flags.bucket}" (concurrency: ${flags.concurrency})`
      ));

      if (flags.prefix) {
         this.logInfoToStderr(`${chalk.gray('Prefix:')} ${flags.prefix}`);
      }

      if (isDryRun) {
         this.logInfoToStderr(chalk.yellow('Dry run — no objects will be deleted'));
      }

      for await (const batch of batchRecords(listAllVersions(client, flags.bucket, flags.prefix), BATCH_SIZE)) {
         if (isDryRun) {
            for (const obj of batch) {
               this.log(JSON.stringify({ key: obj.Key, versionId: obj.VersionId }));
            }
            counters.deleted += batch.length;
            continue;
         }

         queue.add(async () => {
            await sendDeleteBatch(client, flags.bucket, batch, ctx);
         });

         if (queue.size > maxQueueSize) {
            await queue.onEmpty();
         }
      }

      await queue.onIdle();

      this.logInfoToStderr(chalk.gray('Checking for in-progress multipart uploads...'));

      for await (const upload of listMultipartUploads(client, flags.bucket, flags.prefix)) {
         if (isDryRun) {
            this.log(JSON.stringify({ key: upload.Key, uploadId: upload.UploadId }));
            counters.abortedUploads += 1;
            continue;
         }

         queue.add(async () => {
            await abortUpload(client, flags.bucket, upload, ctx);
         });

         if (queue.size > maxQueueSize) {
            await queue.onEmpty();
         }
      }

      await queue.onIdle();

      const objectsTotal = counters.deleted + counters.failed,
            deletedVerb = isDryRun ? 'would delete' : 'deleted',
            abortedVerb = isDryRun ? 'would abort' : 'aborted';

      this.logToStderr(chalk.whiteBright(
         `Total: ${objectsTotal} objects (${counters.deleted} ${deletedVerb} / ${counters.failed} failed)`
      ));
      this.logToStderr(chalk.whiteBright(
         `Multipart uploads: ${counters.abortedUploads} ${abortedVerb} / ${counters.failedAborts} failed`
      ));

      if (counters.failed + counters.failedAborts > 0) {
         this.exit(1);
      }
   }

}
