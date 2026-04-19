import toMessage from '../to-message.js';
import { ScanCursorState, writeCursorFileAtomic } from './scan-cursor-file.js';

export interface CursorFlusherArgs {
   path: string | undefined;
   state: ScanCursorState;
   onError: (message: string) => void;
}

export interface CursorFlusher {
   schedule: () => void;
   drain: () => Promise<void>;
   hasFailed: () => boolean;
}

export default function createCursorFlusher(args: CursorFlusherArgs): CursorFlusher {
   const { path, state, onError } = args;

   let dirty = false,
       active: Promise<void> | null = null,
       failed = false;

   const runLoop = async (safePath: string): Promise<void> => {
      while (dirty && !failed) {
         dirty = false;
         try {
            await writeCursorFileAtomic(safePath, state);
         } catch(e) {
            failed = true;
            onError(toMessage(e));
            return;
         }
      }
   };

   const schedule = (): void => {
      if (!path || failed) {
         return;
      }
      dirty = true;
      if (active === null) {
         active = runLoop(path).finally(() => { active = null; });
      }
   };

   const awaitAllInflight = async (): Promise<void> => {
      let inflight = active;

      while (inflight !== null) {
         await inflight;
         inflight = active;
      }
   };

   const drain = async (): Promise<void> => {
      await awaitAllInflight();
      if (dirty && !failed) {
         schedule();
         await awaitAllInflight();
      }
   };

   const hasFailed = (): boolean => {
      return failed;
   };

   return { schedule, drain, hasFailed };
}
