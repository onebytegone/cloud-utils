import { WriteStream } from 'fs';
import { Writable } from 'stream';
import {
   AttributeDefinition,
   DescribeTableCommand,
   DynamoDBClient,
   KeySchemaElement,
   paginateQuery,
   QueryCommandInput,
   ReturnConsumedCapacity,
} from '@aws-sdk/client-dynamodb';
import { Flags } from '@oclif/core';
import { isString } from '@silvermine/toolbox';
import chalk from 'chalk';
import { BaseCommand } from '../../base-command.js';
import createWriteStream from '../../lib/create-write-stream.js';
import endWriteStream from '../../lib/end-write-stream.js';
import toMessage from '../../lib/to-message.js';
import emitItemNDJSON from '../../lib/dynamodb/emit-item-ndjson.js';
import {
   buildQueryInput,
   KeyAttrType,
   KeySchemaInfo,
   parseSkFlags,
   RawSkFlags,
} from '../../lib/dynamodb/build-query-input.js';

function findAttrType(defs: AttributeDefinition[], name: string): KeyAttrType {
   const def = defs.find((entry) => { return entry.AttributeName === name; });

   if (!def || !def.AttributeType) {
      throw new Error(`Missing attribute definition for "${name}"`);
   }

   if (def.AttributeType !== 'S' && def.AttributeType !== 'N' && def.AttributeType !== 'B') {
      throw new Error(
         `Unsupported attribute type "${def.AttributeType}" for "${name}" `
         + '(only S, N, B are supported)'
      );
   }

   return def.AttributeType;
}

async function resolveKeySchema(
   client: DynamoDBClient,
   tableName: string,
   indexName: string | undefined
): Promise<KeySchemaInfo> {
   const response = await client.send(new DescribeTableCommand({ TableName: tableName })),
         table = response.Table;

   if (!table || !table.AttributeDefinitions || !table.KeySchema) {
      throw new Error(`DescribeTable returned no usable schema for "${tableName}"`);
   }

   let keySchema: KeySchemaElement[] = table.KeySchema;

   if (indexName) {
      const gsis = table.GlobalSecondaryIndexes || [],
            lsis = table.LocalSecondaryIndexes || [];

      const match = gsis.find((idx) => { return idx.IndexName === indexName; })
                 || lsis.find((idx) => { return idx.IndexName === indexName; });

      if (!match || !match.KeySchema) {
         const existing = [ ...gsis, ...lsis ]
            .map((idx) => { return idx.IndexName; })
            .filter(isString)
            .join(', ');

         throw new Error(
            `Index "${indexName}" not found on table "${tableName}". `
            + `Existing indexes: ${existing || '(none)'}`
         );
      }

      keySchema = match.KeySchema;
   }

   const pkEntry = keySchema.find((entry) => { return entry.KeyType === 'HASH'; }),
         skEntry = keySchema.find((entry) => { return entry.KeyType === 'RANGE'; });

   if (!pkEntry || !pkEntry.AttributeName) {
      const where = indexName ? `index "${indexName}"` : `table "${tableName}"`;

      throw new Error(`No partition key defined for ${where}`);
   }

   const defs = table.AttributeDefinitions;

   return {
      pkName: pkEntry.AttributeName,
      pkType: findAttrType(defs, pkEntry.AttributeName),
      sk: skEntry?.AttributeName
         ? { name: skEntry.AttributeName, type: findAttrType(defs, skEntry.AttributeName) }
         : undefined,
   };
}

interface EmitArgs {
   client: DynamoDBClient;
   queryInput: QueryCommandInput;
   sink: Writable;
   outputStream: WriteStream | undefined;
   limit: number | undefined;
   reportRcu: boolean;
}

export default class Query extends BaseCommand {

   public static summary = 'Query a DynamoDB table or secondary index; emits NDJSON';

   public static flags = {
      table: Flags.string({
         description: 'name of the table',
         required: true,
      }),
      index: Flags.string({
         description: 'name of the GSI or LSI to query; omit to query the base table',
      }),
      pk: Flags.string({
         description: 'partition key value',
         required: true,
      }),
      sk: Flags.string({
         description: 'sort key value (equals)',
      }),
      'sk-lt': Flags.string({
         description: 'sort key less than',
      }),
      'sk-lte': Flags.string({
         description: 'sort key less than or equal',
      }),
      'sk-gt': Flags.string({
         description: 'sort key greater than',
      }),
      'sk-gte': Flags.string({
         description: 'sort key greater than or equal; combine with --sk-lte for BETWEEN',
      }),
      'sk-prefix': Flags.string({
         description: 'sort key begins_with',
      }),
      limit: Flags.integer({
         description: 'soft floor on items emitted; paginates until at least this many are '
            + 'written, extras from the final page are still emitted',
      }),
      reverse: Flags.boolean({
         description: 'descending sort (ScanIndexForward: false)',
         default: false,
      }),
      output: Flags.string({
         char: 'o',
         description: 'write NDJSON to this file instead of stdout',
      }),
      rcu: Flags.boolean({
         description: 'report total consumed read capacity units (RCUs) to stderr on completion',
         default: false,
      }),
   };

   public async run(): Promise<void> {
      const { flags } = await this.parse(Query);

      const rawSk: RawSkFlags = {
         eq: flags.sk,
         lt: flags['sk-lt'],
         lte: flags['sk-lte'],
         gt: flags['sk-gt'],
         gte: flags['sk-gte'],
         prefix: flags['sk-prefix'],
      };

      const skCondition = this.runOrExit(() => { return parseSkFlags(rawSk); }, 2),
            client = new DynamoDBClient({ region: flags.region });

      const keySchema = await this.runAsyncOrExit(
         () => { return resolveKeySchema(client, flags.table, flags.index); },
         1
      );

      if (skCondition.kind !== 'none' && !keySchema.sk) {
         const where = flags.index ? `index "${flags.index}"` : `table "${flags.table}"`;

         this.error(
            `--sk-* flag(s) supplied, but ${where} has no sort key in its key schema`,
            { exit: 1 }
         );
      }

      const queryInput = this.runOrExit(() => {
         return buildQueryInput({
            tableName: flags.table,
            indexName: flags.index,
            pkValue: flags.pk,
            skCondition,
            keySchema,
            reverse: flags.reverse,
         });
      }, 2);

      if (flags.rcu) {
         queryInput.ReturnConsumedCapacity = ReturnConsumedCapacity.TOTAL;
      }

      this.logToStderr(chalk.gray(
         `Querying ${flags.table}${flags.index ? ` / ${flags.index}` : ''}...`
      ));

      const outputStream: WriteStream | undefined = flags.output
         ? await createWriteStream(flags.output)
         : undefined;

      const sink: Writable = outputStream || process.stdout;

      await this.emitResults({
         client, queryInput, sink, outputStream, limit: flags.limit, reportRcu: flags.rcu,
      });
   }

   private runOrExit<T>(fn: () => T, exitCode: number): T {
      try {
         return fn();
      } catch(e) {
         return this.error(toMessage(e), { exit: exitCode });
      }
   }

   private async runAsyncOrExit<T>(fn: () => Promise<T>, exitCode: number): Promise<T> {
      try {
         return await fn();
      } catch(e) {
         return this.error(toMessage(e), { exit: exitCode });
      }
   }

   private async emitResults(args: EmitArgs): Promise<void> {
      const { client, queryInput, sink, outputStream, limit, reportRcu } = args;

      let emitted = 0,
          totalRcu = 0;

      try {
         try {
            for await (const page of paginateQuery({ client }, queryInput)) {
               if (page.ConsumedCapacity?.CapacityUnits !== undefined) {
                  totalRcu += page.ConsumedCapacity.CapacityUnits;
               }

               for (const item of page.Items || []) {
                  await emitItemNDJSON(sink, item);
                  emitted += 1;
               }

               if (limit !== undefined && emitted >= limit) {
                  break;
               }
            }
         } finally {
            if (outputStream) {
               await endWriteStream(outputStream);
            }
         }
      } catch(e) {
         this.logToStderr(chalk.red(`Query failed after ${emitted} items: ${toMessage(e)}`));
         this.exit(1);
      }

      this.logToStderr(chalk.gray(`Done. ${emitted} item(s).`));

      if (reportRcu) {
         this.logToStderr(chalk.gray(`Consumed capacity: ${totalRcu.toFixed(2)} RCU`));
      }
   }

}
