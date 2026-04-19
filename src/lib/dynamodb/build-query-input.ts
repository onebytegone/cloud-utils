import { AttributeValue, QueryCommandInput } from '@aws-sdk/client-dynamodb';

export type KeyAttrType = 'S' | 'N' | 'B';

export interface KeySchemaInfo {
   pkName: string;
   pkType: KeyAttrType;
   sk?: {
      name: string;
      type: KeyAttrType;
   };
}

export type SkCondition =
   | { kind: 'none' }
   | { kind: 'eq' | 'lt' | 'lte' | 'gt' | 'gte' | 'prefix'; value: string }
   | { kind: 'between'; gte: string; lte: string };

export interface RawSkFlags {
   eq?: string;
   lt?: string;
   lte?: string;
   gt?: string;
   gte?: string;
   prefix?: string;
}

export interface BuildQueryInputArgs {
   tableName: string;
   indexName?: string;
   pkValue: string;
   skCondition: SkCondition;
   keySchema: KeySchemaInfo;
   reverse: boolean;
}

const VALID_SHAPES_MSG = [
   'Valid --sk-* combinations are:',
   '  - no --sk-* flags (partition-key-only query)',
   '  - exactly one of: --sk, --sk-lt, --sk-lte, --sk-gt, --sk-gte, --sk-prefix',
   '  - --sk-gte combined with --sk-lte (inclusive BETWEEN)',
]
   .join('\n');

export function parseSkFlags(flags: RawSkFlags): SkCondition {
   const providedFlagNames = Object.entries(flags)
      .filter(([ , value ]) => { return value !== undefined; })
      .map(([ key ]) => { return key; });

   const count = providedFlagNames.length;

   if (count === 0) {
      return { kind: 'none' };
   }

   if (count === 2 && flags.gte !== undefined && flags.lte !== undefined) {
      return { kind: 'between', gte: flags.gte, lte: flags.lte };
   }

   if (count === 1) {
      if (flags.eq !== undefined) {
         return { kind: 'eq', value: flags.eq };
      }
      if (flags.lt !== undefined) {
         return { kind: 'lt', value: flags.lt };
      }
      if (flags.lte !== undefined) {
         return { kind: 'lte', value: flags.lte };
      }
      if (flags.gt !== undefined) {
         return { kind: 'gt', value: flags.gt };
      }
      if (flags.gte !== undefined) {
         return { kind: 'gte', value: flags.gte };
      }
      if (flags.prefix !== undefined) {
         return { kind: 'prefix', value: flags.prefix };
      }
   }

   throw new Error(
      `Invalid combination of --sk-* flags: [${providedFlagNames.join(', ')}].\n${VALID_SHAPES_MSG}`
   );
}

function coerceValue(raw: string, type: KeyAttrType, flagName: string): AttributeValue {
   if (type === 'S') {
      return { S: raw };
   }

   if (type === 'N') {
      if (!Number.isFinite(Number(raw))) {
         throw new Error(
            `Value for ${flagName} must be a valid number (key attribute type is N). `
            + `Got: "${raw}"`
         );
      }

      return { N: raw };
   }

   // Buffer.from with invalid chars silently skips them; round-trip and compare
   // (ignoring padding) so a typo fails loudly, not as a silent wrong-key query.
   const buf = Buffer.from(raw, 'base64'),
         stripped = raw.replace(/=+$/, ''),
         roundTrip = buf.toString('base64').replace(/=+$/, '');

   if (roundTrip !== stripped) {
      throw new Error(
         `Value for ${flagName} must be valid base64 (key attribute type is B). `
         + `Got: "${raw}"`
      );
   }

   return { B: new Uint8Array(buf) };
}

export function buildQueryInput(args: BuildQueryInputArgs): QueryCommandInput {
   const { tableName, indexName, pkValue, skCondition, keySchema, reverse } = args;

   const pkAttr = coerceValue(pkValue, keySchema.pkType, '--pk'),
         names: Record<string, string> = { '#pk': keySchema.pkName },
         values: Record<string, AttributeValue> = { ':pk': pkAttr };

   let expression = '#pk = :pk';

   if (skCondition.kind !== 'none') {
      if (!keySchema.sk) {
         throw new Error('SK flags supplied but resolved key schema has no sort key');
      }

      names['#sk'] = keySchema.sk.name;
      const skType = keySchema.sk.type;

      switch (skCondition.kind) {
         case 'between': {
            values[':skLo'] = coerceValue(skCondition.gte, skType, '--sk-gte');
            values[':skHi'] = coerceValue(skCondition.lte, skType, '--sk-lte');
            expression += ' AND #sk BETWEEN :skLo AND :skHi';
            break;
         }
         case 'eq': {
            values[':sk'] = coerceValue(skCondition.value, skType, '--sk');
            expression += ' AND #sk = :sk';
            break;
         }
         case 'lt': {
            values[':sk'] = coerceValue(skCondition.value, skType, '--sk-lt');
            expression += ' AND #sk < :sk';
            break;
         }
         case 'lte': {
            values[':sk'] = coerceValue(skCondition.value, skType, '--sk-lte');
            expression += ' AND #sk <= :sk';
            break;
         }
         case 'gt': {
            values[':sk'] = coerceValue(skCondition.value, skType, '--sk-gt');
            expression += ' AND #sk > :sk';
            break;
         }
         case 'gte': {
            values[':sk'] = coerceValue(skCondition.value, skType, '--sk-gte');
            expression += ' AND #sk >= :sk';
            break;
         }
         case 'prefix': {
            values[':sk'] = coerceValue(skCondition.value, skType, '--sk-prefix');
            expression += ' AND begins_with(#sk, :sk)';
            break;
         }
         default: {
            throw new Error('Unreachable SkCondition kind');
         }
      }
   }

   return {
      TableName: tableName,
      IndexName: indexName,
      KeyConditionExpression: expression,
      ExpressionAttributeNames: names,
      ExpressionAttributeValues: values,
      ScanIndexForward: !reverse,
   };
}
