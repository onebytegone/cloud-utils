import { readFile, rename, unlink, writeFile } from 'fs/promises';
import { AttributeValue } from '@aws-sdk/client-dynamodb';
import {
   isArray,
   isBoolean,
   isNumber,
   isString,
   isStringUnknownMap,
   range,
} from '@silvermine/toolbox';


export type SegmentState =
   | { index: number; status: 'pending' }
   | {
      index: number;
      status: 'in_progress';
      exclusiveStartKey: Record<string, AttributeValue>;
   }
   | { index: number; status: 'completed' }
   | {
      index: number;
      status: 'failed';
      reason: string;
      exclusiveStartKey?: Record<string, AttributeValue>;
   };

export interface ScanCursorState {
   table: string;
   index: string | null;
   outputPath: string | null;
   totalSegments: number;
   emitted: number;
   segments: SegmentState[];
}

export interface ScanIdentity {
   table: string;
   index: string | undefined;
   outputPath: string | undefined;
   segments: number;
}

// AttributeValue is a compile-time discriminated union in @aws-sdk/client-dynamodb;
// no runtime key list is exported, so enumerate the tags here.
const ATTRIBUTE_VALUE_KEYS = [
   'S', 'N', 'B', 'SS', 'NS', 'BS', 'L', 'M', 'NULL', 'BOOL', '$unknown',
] as const;

type AttributeValueKey = typeof ATTRIBUTE_VALUE_KEYS[number];

function isAttributeValueKey(key: string): key is AttributeValueKey {
   return (ATTRIBUTE_VALUE_KEYS as readonly string[]).includes(key);
}

function isENOENT(error: unknown): boolean {
   return isStringUnknownMap(error) && error.code === 'ENOENT';
}

function decodeBase64(raw: string): Uint8Array {
   return new Uint8Array(Buffer.from(raw, 'base64'));
}

function parseBinary(payload: unknown): Uint8Array {
   if (payload instanceof Uint8Array) {
      return payload;
   }
   if (isString(payload)) {
      return decodeBase64(payload);
   }
   throw new Error('binary payload must be Uint8Array or base64 string');
}

// eslint-disable-next-line complexity
function parseAttributeValue(value: unknown): AttributeValue {
   if (!isStringUnknownMap(value)) {
      throw new Error('not an AttributeValue object');
   }

   const tags = Object.keys(value).filter(isAttributeValueKey);

   if (tags.length !== 1) {
      throw new Error(
         `AttributeValue must have exactly one tag (got: [${Object.keys(value).join(', ')}])`
      );
   }

   const tag = tags[0],
         payload = value[tag];

   switch (tag) {
      case 'S': {
         if (!isString(payload)) {
            throw new Error('S payload must be string');
         }
         return { S: payload };
      }
      case 'N': {
         if (!isString(payload)) {
            throw new Error('N payload must be numeric string');
         }
         return { N: payload };
      }
      case 'B': {
         return { B: parseBinary(payload) };
      }
      case 'SS': {
         if (!isArray(payload) || !payload.every(isString)) {
            throw new Error('SS payload must be string[]');
         }
         return { SS: payload };
      }
      case 'NS': {
         if (!isArray(payload) || !payload.every(isString)) {
            throw new Error('NS payload must be numeric-string[]');
         }
         return { NS: payload };
      }
      case 'BS': {
         if (!isArray(payload)) {
            throw new Error('BS payload must be an array');
         }
         return { BS: payload.map(parseBinary) };
      }
      case 'BOOL': {
         if (!isBoolean(payload)) {
            throw new Error('BOOL payload must be boolean');
         }
         return { BOOL: payload };
      }
      case 'NULL': {
         if (!isBoolean(payload)) {
            throw new Error('NULL payload must be boolean');
         }
         return { NULL: payload };
      }
      case 'L': {
         if (!isArray(payload)) {
            throw new Error('L payload must be an array');
         }
         return { L: payload.map(parseAttributeValue) };
      }
      case 'M': {
         if (!isStringUnknownMap(payload)) {
            throw new Error('M payload must be an object');
         }
         const result: Record<string, AttributeValue> = {};

         for (const [ k, v ] of Object.entries(payload)) {
            result[k] = parseAttributeValue(v);
         }
         return { M: result };
      }
      case '$unknown': {
         throw new Error('$unknown AttributeValue is not supported in cursor files');
      }
      default: {
         throw new Error(`Unknown AttributeValue tag: ${tag}`);
      }
   }
}

function toAttributeValueMap(value: unknown): Record<string, AttributeValue> {
   if (!isStringUnknownMap(value)) {
      throw new Error('exclusiveStartKey is not an object');
   }

   const result: Record<string, AttributeValue> = {};

   for (const [ k, v ] of Object.entries(value)) {
      try {
         result[k] = parseAttributeValue(v);
      } catch(e) {
         const msg = e instanceof Error ? e.message : String(e);

         throw new Error(`exclusiveStartKey["${k}"]: ${msg}`);
      }
   }

   return result;
}

// eslint-disable-next-line complexity
function parseSegment(raw: unknown, expectedIndex: number, path: string): SegmentState {
   if (!isStringUnknownMap(raw)) {
      throw new Error(`Cursor file "${path}": segments[${expectedIndex}] is not an object`);
   }

   const seg: Record<string, unknown> = raw;

   if (seg.index !== expectedIndex) {
      throw new Error(
         `Cursor file "${path}": segments[${expectedIndex}].index must equal ${expectedIndex}`
      );
   }

   const status: unknown = seg.status;

   if (status === undefined) {
      throw new Error(
         `Cursor file "${path}": segments[${expectedIndex}].status is required`
      );
   }

   const rawESK: unknown = seg.exclusiveStartKey;

   const rawKey = rawESK !== null && rawESK !== undefined
      ? toAttributeValueMap(rawESK)
      : undefined;

   if (status === 'pending') {
      return { index: expectedIndex, status: 'pending' };
   }

   if (status === 'in_progress') {
      if (!rawKey) {
         throw new Error(
            `Cursor file "${path}": segments[${expectedIndex}].exclusiveStartKey `
            + 'is required when status is "in_progress"'
         );
      }
      return { index: expectedIndex, status: 'in_progress', exclusiveStartKey: rawKey };
   }

   if (status === 'completed') {
      return { index: expectedIndex, status: 'completed' };
   }

   if (status === 'failed') {
      const rawReason: unknown = seg.reason,
            reason = isString(rawReason) ? rawReason : 'unknown';

      return rawKey
         ? { index: expectedIndex, status: 'failed', reason, exclusiveStartKey: rawKey }
         : { index: expectedIndex, status: 'failed', reason };
   }

   throw new Error(
      `Cursor file "${path}": segments[${expectedIndex}].status must be one of `
      + '"pending", "in_progress", "completed", "failed"'
   );
}

// eslint-disable-next-line complexity
export async function readCursorFile(path: string): Promise<ScanCursorState | null> {
   let raw: string;

   try {
      raw = await readFile(path, 'utf-8');
   } catch(e) {
      if (isENOENT(e)) {
         return null;
      }
      throw e;
   }

   let parsed: unknown;

   try {
      parsed = JSON.parse(raw);
   } catch(e) {
      const msg = e instanceof Error ? e.message : String(e);

      throw new Error(`Cursor file "${path}" is not valid JSON: ${msg}`);
   }

   if (!isStringUnknownMap(parsed)) {
      throw new Error(`Cursor file "${path}" root is not an object`);
   }

   const obj: Record<string, unknown> = parsed;

   const rawTable: unknown = obj.table;

   if (!isString(rawTable)) {
      throw new Error(`Cursor file "${path}": "table" must be a string`);
   }

   const table: string = rawTable,
         rawIndex: unknown = obj.index;

   if (rawIndex === undefined) {
      throw new Error(`Cursor file "${path}": "index" must be a string or null`);
   }

   if (rawIndex !== null && !isString(rawIndex)) {
      throw new Error(`Cursor file "${path}": "index" must be a string or null`);
   }

   const index: string | null = isString(rawIndex) ? rawIndex : null;

   const rawOutputPath: unknown = obj.outputPath;

   if (rawOutputPath !== undefined && rawOutputPath !== null && !isString(rawOutputPath)) {
      throw new Error(`Cursor file "${path}": "outputPath" must be a string or null`);
   }

   const outputPath: string | null = isString(rawOutputPath) ? rawOutputPath : null;

   const rawTotalSegments: unknown = obj.totalSegments;

   if (!isNumber(rawTotalSegments) || !Number.isInteger(rawTotalSegments) || rawTotalSegments < 1) {
      throw new Error(`Cursor file "${path}": "totalSegments" must be a positive integer`);
   }

   const totalSegments: number = rawTotalSegments;

   const rawEmitted: unknown = obj.emitted,
         emitted = rawEmitted === undefined ? 0 : rawEmitted;

   if (!isNumber(emitted) || !Number.isInteger(emitted) || emitted < 0) {
      throw new Error(`Cursor file "${path}": "emitted" must be a non-negative integer`);
   }

   const rawSegments: unknown = obj.segments;

   if (!isArray(rawSegments)) {
      throw new Error(`Cursor file "${path}": "segments" must be an array`);
   }

   if (rawSegments.length !== totalSegments) {
      throw new Error(
         `Cursor file "${path}": segments.length (${rawSegments.length}) `
         + `!= totalSegments (${totalSegments})`
      );
   }

   const segments: SegmentState[] = rawSegments.map((rawSeg, idx) => {
      return parseSegment(rawSeg, idx, path);
   });

   return {
      table,
      index,
      outputPath,
      totalSegments,
      emitted,
      segments,
   };
}

// JSON.stringify renders Uint8Array as {"0":n,...}; encode as base64 so binary
// key attributes in exclusiveStartKey round-trip correctly on resume.
function cursorReplacer(_key: string, value: unknown): unknown {
   if (value instanceof Uint8Array) {
      return Buffer.from(value).toString('base64');
   }
   return value;
}

export async function writeCursorFileAtomic(path: string, state: ScanCursorState): Promise<void> {
   const tmp = `${path}.tmp`,
         json = JSON.stringify(state, cursorReplacer) + '\n';

   try {
      await writeFile(tmp, json, 'utf-8');
      await rename(tmp, path);
   } catch(e) {
      await unlink(tmp).catch(() => { /* tmp may not exist; best-effort cleanup */ });
      throw e;
   }
}

export async function deleteCursorFile(path: string): Promise<void> {
   try {
      await unlink(path);
   } catch(e) {
      if (isENOENT(e)) {
         return;
      }
      throw e;
   }
}

export function isSegmentRunnable(seg: SegmentState): boolean {
   return seg.status === 'pending' || seg.status === 'in_progress' || seg.status === 'failed';
}

export function runnableSegmentIndices(state: ScanCursorState): number[] {
   return state.segments.filter(isSegmentRunnable).map((seg) => { return seg.index; });
}

export function completedSegmentCount(state: ScanCursorState): number {
   return state.segments
      .filter((seg) => { return seg.status === 'completed'; })
      .length;
}

export function initScanCursorState(
   existing: ScanCursorState | null,
   identity: ScanIdentity
): ScanCursorState {
   if (existing) {
      return existing;
   }

   const segments: SegmentState[] = range(identity.segments).map((i) => {
      return { index: i, status: 'pending' };
   });

   return {
      table: identity.table,
      index: identity.index || null,
      outputPath: identity.outputPath || null,
      totalSegments: identity.segments,
      emitted: 0,
      segments,
   };
}

export function segmentStartKey(
   seg: SegmentState
): Record<string, AttributeValue> | undefined {
   if (seg.status === 'in_progress') {
      return seg.exclusiveStartKey;
   }
   if (seg.status === 'failed') {
      return seg.exclusiveStartKey;
   }
   return undefined;
}

export function validateCursorMatches(state: ScanCursorState, identity: ScanIdentity): void {
   const flagIndex = identity.index || null,
         flagOutput = identity.outputPath || null;

   if (state.table !== identity.table) {
      throw new Error(
         `Cursor file was produced for table="${state.table}"; `
         + `got --table="${identity.table}". Refusing to resume.`
      );
   }

   if (state.index !== flagIndex) {
      throw new Error(
         `Cursor file was produced for index=${JSON.stringify(state.index)}; `
         + `got --index=${JSON.stringify(flagIndex)}. Refusing to resume.`
      );
   }

   if (state.outputPath !== flagOutput) {
      throw new Error(
         `Cursor file was produced for output=${JSON.stringify(state.outputPath)}; `
         + `got --output=${JSON.stringify(flagOutput)}. Refusing to resume.`
      );
   }

   if (state.totalSegments !== identity.segments) {
      throw new Error(
         `Cursor file was produced for totalSegments=${state.totalSegments}; `
         + `got --segments=${identity.segments}. Refusing to resume.`
      );
   }
}
