import { WriteStream } from 'fs';
import { Writable } from 'stream';
import { DynamoDBClient, paginateScan, ScanCommandInput } from '@aws-sdk/client-dynamodb';
import { Flags } from '@oclif/core';
import chalk from 'chalk';
import PQueue from 'p-queue';
import { BaseCommand } from '../../base-command.js';
import createWriteStream from '../../lib/create-write-stream.js';
import endWriteStream from '../../lib/end-write-stream.js';
import toMessage from '../../lib/to-message.js';
import createCursorFlusher from '../../lib/dynamodb/cursor-flusher.js';
import emitItemNDJSON from '../../lib/dynamodb/emit-item-ndjson.js';
import {
   completedSegmentCount,
   deleteCursorFile,
   initScanCursorState,
   readCursorFile,
   runnableSegmentIndices,
   ScanCursorState,
   ScanIdentity,
   segmentStartKey,
   validateCursorMatches,
} from '../../lib/dynamodb/scan-cursor-file.js';

const STATUS_INTERVAL = 1000;

// Randomize the segment run order so concurrent resumers of the same scan do
// not hammer identical DynamoDB partitions in lock-step, which would compound
// throttling on hot partitions.
function shuffleInPlace<T>(arr: T[]): void {
   for (let i = arr.length - 1; i > 0; i -= 1) {
      const j = Math.floor(Math.random() * (i + 1)),
            tmp = arr[i];

      arr[i] = arr[j];
      arr[j] = tmp;
   }
}

export default class Scan extends BaseCommand {

   public static summary = 'Parallel segmented scan of a DynamoDB table; emits NDJSON. Output is NOT ordered.';

   public static description = 'Scans every item in a table or secondary index in parallel across N '
      + 'segments. Items are emitted as unmarshalled NDJSON, one per line, in arrival '
      + 'order (NOT sorted). Use --output to write to a file instead of stdout; status '
      + 'and errors go to stderr. Pass --cursor-file to enable resumable scans; on a '
      + 'clean crash mid-page, resume may replay that page (at-least-once semantics).';

   public static flags = {
      table: Flags.string({
         description: 'name of the table',
         required: true,
      }),
      index: Flags.string({
         description: 'name of the GSI or LSI to scan; omit to scan the base table',
      }),
      segments: Flags.integer({
         description: 'number of parallel scan segments (TotalSegments)',
         default: 10,
         min: 1,
      }),
      concurrency: Flags.integer({
         description: 'max number of segments scanned simultaneously',
         default: 10,
         min: 1,
      }),
      output: Flags.string({
         char: 'o',
         description: 'write NDJSON to this file instead of stdout',
      }),
      'cursor-file': Flags.string({
         description: 'path to a cursor state file; enables resumable scan. '
            + 'If the file exists: resume from it and overwrite with new state. '
            + 'If absent: fresh scan that writes state here. '
            + 'Deleted on clean completion.',
      }),
   };

   // eslint-disable-next-line complexity
   public async run(): Promise<void> {
      const { flags } = await this.parse(Scan),
            cursorPath = flags['cursor-file'];

      const identity: ScanIdentity = {
         table: flags.table,
         index: flags.index,
         outputPath: flags.output,
         segments: flags.segments,
      };

      const existingCursor = cursorPath ? await readCursorFile(cursorPath) : null;

      if (existingCursor) {
         try {
            validateCursorMatches(existingCursor, identity);
         } catch(e) {
            this.error(toMessage(e), { exit: 2 });
         }
      }

      const state = initScanCursorState(existingCursor, identity),
            indices = runnableSegmentIndices(state);

      shuffleInPlace(indices);

      if (existingCursor) {
         this.logToStderr(chalk.gray(
            `Resuming from ${cursorPath}: ${indices.length}/${flags.segments} segments remaining`
         ));
      }

      const client = new DynamoDBClient({ region: flags.region, maxAttempts: 10 });

      const outputStream: WriteStream | undefined = flags.output
         ? await createWriteStream(flags.output, { append: existingCursor !== null })
         : undefined;

      const sink: Writable = outputStream || process.stdout;

      const outcome = await this.runScan({
         state, indices, cursorPath, client, sink, outputStream, flags,
      });

      await this.finalize({ outcome, cursorPath, state });
   }

   private async runScan(args: {
      state: ScanCursorState;
      indices: number[];
      cursorPath: string | undefined;
      client: DynamoDBClient;
      sink: Writable;
      outputStream: WriteStream | undefined;
      flags: {
         table: string;
         index: string | undefined;
         segments: number;
         concurrency: number;
      };
   }): Promise<{ failedSegments: number; shouldStop: boolean; cursorFlushFailed: boolean }> {
      const { state, indices, cursorPath, client, sink, outputStream, flags } = args;

      let failedSegments = 0,
          shouldStop = false;

      const queue = new PQueue({ concurrency: flags.concurrency });

      const flusher = createCursorFlusher({
         path: cursorPath,
         state,
         onError: (msg) => {
            shouldStop = true;
            this.logToStderr(chalk.red(`Cursor flush failed: ${msg}`));
         },
      });

      queue.on('error', (e: unknown) => {
         failedSegments += 1;
         this.logToStderr(chalk.red(`Segment task error: ${toMessage(e)}`));
      });

      const signalHandler = (): void => {
         if (shouldStop) {
            this.logToStderr(chalk.red(
               'Second interrupt received; aborting further work. Send SIGKILL if needed.'
            ));
            return;
         }
         shouldStop = true;
         queue.clear();
         this.logToStderr(chalk.yellow(
            'Interrupt received; waiting for in-flight pages to finish and flushing cursor...'
         ));
      };

      process.on('SIGINT', signalHandler);
      process.on('SIGTERM', signalHandler);

      this.logToStderr(chalk.gray(
         `Scanning ${flags.table}${flags.index ? ` / ${flags.index}` : ''} `
         + `with ${flags.segments} segments, ${flags.concurrency} concurrent...`
      ));

      const runSegment = async (segmentIndex: number): Promise<void> => {
         const startKey = segmentStartKey(state.segments[segmentIndex]);

         try {
            const params: ScanCommandInput = {
               TableName: flags.table,
               IndexName: flags.index,
               TotalSegments: flags.segments,
               Segment: segmentIndex,
               ExclusiveStartKey: startKey,
            };

            for await (const page of paginateScan({ client }, params)) {
               if (shouldStop) {
                  break;
               }

               for (const item of page.Items || []) {
                  await emitItemNDJSON(sink, item);
                  state.emitted += 1;

                  if (state.emitted % STATUS_INTERVAL === 0) {
                     this.logToStderr(chalk.gray(
                        `Progress: ${state.emitted} items, `
                        + `${completedSegmentCount(state)}/${flags.segments} segments`
                     ));
                  }
               }

               state.segments[segmentIndex] = page.LastEvaluatedKey
                  ? {
                     index: segmentIndex,
                     status: 'in_progress',
                     exclusiveStartKey: page.LastEvaluatedKey,
                  }
                  : { index: segmentIndex, status: 'completed' };

               flusher.schedule();
            }
         } catch(e) {
            failedSegments += 1;

            const current = state.segments[segmentIndex],
                  currentKey = segmentStartKey(current),
                  reason = toMessage(e);

            state.segments[segmentIndex] = currentKey
               ? { index: segmentIndex, status: 'failed', reason, exclusiveStartKey: currentKey }
               : { index: segmentIndex, status: 'failed', reason };

            flusher.schedule();
            this.logToStderr(chalk.red(`Segment ${segmentIndex} failed: ${reason}`));
         }
      };

      const enqueueSegment = (segmentIndex: number): void => {
         queue
            .add(() => { return runSegment(segmentIndex); })
            .catch(() => { /* rejection also delivered via queue 'error' handler above */ });
      };

      try {
         try {
            for (const segmentIndex of indices) {
               enqueueSegment(segmentIndex);
            }

            await queue.onIdle();
            await flusher.drain();
         } finally {
            if (outputStream && !outputStream.destroyed) {
               await endWriteStream(outputStream);
            }

            process.off('SIGINT', signalHandler);
            process.off('SIGTERM', signalHandler);
         }
      } catch(e) {
         this.logToStderr(chalk.red(`Scan failed: ${toMessage(e)}`));
         this.exit(1);
      }

      return { failedSegments, shouldStop, cursorFlushFailed: flusher.hasFailed() };
   }

   private async finalize(args: {
      outcome: { failedSegments: number; shouldStop: boolean; cursorFlushFailed: boolean };
      cursorPath: string | undefined;
      state: ScanCursorState;
   }): Promise<void> {
      const { outcome, cursorPath, state } = args;

      const completed = completedSegmentCount(state);

      const cleanComplete = completed === state.totalSegments
         && outcome.failedSegments === 0
         && !outcome.shouldStop
         && !outcome.cursorFlushFailed;

      this.logToStderr(chalk.gray(
         `Done. ${state.emitted} items, ${completed}/${state.totalSegments} segments succeeded.`
      ));

      if (cursorPath && cleanComplete) {
         await deleteCursorFile(cursorPath);
      } else if (cursorPath) {
         const remaining = runnableSegmentIndices(state);

         this.logToStderr(chalk.yellow(
            `Cursor file kept at "${cursorPath}". Pending segments: [${remaining.join(', ')}]. `
            + 'Re-run with the same flags to resume.'
         ));
      }

      if (!cleanComplete) {
         this.exit(1);
      }
   }

}
