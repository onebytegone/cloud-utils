import { Command } from 'commander';
import { version } from '../package.json';
import registerLambdaBatchInvoke from './commands/lambda/batch-invoke';
import registerSQSDownloadMessages from './commands/sqs/download-messages';
import registerSQSOldestMessageReport from './commands/sqs/oldest-message-report';
import registerStepFunctionsListExecutions from './commands/step-functions/list-executions';

const program = new Command();

program.name('cloud-utils');
program.version(version);

const lambda = program.command('lambda')
   .description('Commands related to AWS Lambda');

registerLambdaBatchInvoke(lambda.command('batch-invoke'));

const sqs = program.command('sqs')
   .description('Commands related to Amazon SQS');

registerSQSDownloadMessages(sqs.command('download-messages'));
registerSQSOldestMessageReport(sqs.command('oldest-message-report'));

const sfn = program.command('sfn') // Step Functions
   .description('Commands related to AWS Step Functions');

registerStepFunctionsListExecutions(sfn.command('list-executions'));

program.parse();
