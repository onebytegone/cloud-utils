import { DateTime } from 'luxon';

export function generateDefaultOutputFilename(...prefix: string[]): string {
   return [ ...prefix, DateTime.utc().toFormat('yyyy-LL-dd\'T\'HHmmss') ].join('-') + '.ndjson';
}
