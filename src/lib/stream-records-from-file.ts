import { extname } from 'path';
import { createReadStream } from 'fs';
import { parse } from 'csv-parse';
import { isEmpty, isStringUnknownMap } from '@silvermine/toolbox';
import { streamLinesFromFile } from './stream-lines-from-file';
import { quitWithError } from './quit-with-error';

type FileFormat = 'ndjson' | 'csv' | 'tsv';

const EXTENSION_TO_FORMAT: Record<string, FileFormat | undefined> = {
   '.ndjson': 'ndjson',
   '.jsonl': 'ndjson',
   '.csv': 'csv',
   '.tsv': 'tsv',
};

function detectFormat(filePath: string): FileFormat {
   const ext = extname(filePath).toLowerCase(),
         format = EXTENSION_TO_FORMAT[ext];

   if (!format) {
      quitWithError(`Unsupported file extension "${ext}". Supported: ${Object.keys(EXTENSION_TO_FORMAT).join(', ')}`);
   }

   return format;
}

async function* streamNdjson(filePath: string): AsyncIterable<Record<string, unknown>> {
   for await (const line of streamLinesFromFile(filePath)) {
      if (isEmpty(line.trim())) {
         continue;
      }

      const parsed: unknown = JSON.parse(line);

      if (!isStringUnknownMap(parsed)) {
         quitWithError(`Expected a JSON object on each line, got: ${line}`);
      }

      yield parsed; // eslint-disable-line no-restricted-syntax
   }
}

async function* streamDelimited(filePath: string, delimiter: string): AsyncIterable<Record<string, unknown>> {
   const parser = createReadStream(filePath).pipe(
      parse({ delimiter, columns: true, skip_empty_lines: true, trim: true })
   );

   for await (const record of parser) {
      if (!isStringUnknownMap(record)) {
         quitWithError(`Expected an object record from parsed file, got: ${JSON.stringify(record)}`);
      }

      yield record; // eslint-disable-line no-restricted-syntax
   }
}

export async function* streamRecordsFromFile(filePath: string): AsyncIterable<Record<string, unknown>> {
   const format = detectFormat(filePath);

   if (format === 'ndjson') {
      yield* streamNdjson(filePath); // eslint-disable-line no-restricted-syntax
      return;
   }

   yield* streamDelimited(filePath, format === 'csv' ? ',' : '\t'); // eslint-disable-line no-restricted-syntax
}
