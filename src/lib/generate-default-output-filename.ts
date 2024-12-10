import { DateTime } from 'luxon';

export function generateDefaultOutputFilenameTimestamp(): string {
   return DateTime.utc().toFormat('yyyy-LL-dd\'T\'HHmmss');
}

export function generateDefaultOutputFilename(...prefix: string[]): string {
   return [ ...prefix, generateDefaultOutputFilenameTimestamp() ].join('-') + '.ndjson';
}
