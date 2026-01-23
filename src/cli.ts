import { Command } from 'commander';
import { version } from '../package.json';
import registerLambdaBulkInvoke from './commands/lambda/bulk-invoke';
import registerLambdaInvoke from './commands/lambda/invoke';
import registerLambdaLogs from './commands/lambda/logs';
import registerSQSDownloadMessages from './commands/sqs/download-messages';
import registerSQSOldestMessageReport from './commands/sqs/oldest-message-report';
import registerStepFunctionsListExecutions from './commands/step-functions/list-executions';
import registerStepFunctionsStartExecutions from './commands/step-functions/start-executions';
import registerTransform from './commands/transform';
import setUpAutocompletion from './lib/set-up-autocompletion';

const program = new Command();

program.name('cloud-utils');
program.version(version);

const lambda = program.command('lambda')
   .description('Commands related to AWS Lambda');

registerLambdaBulkInvoke(lambda.command('bulk-invoke'));
registerLambdaInvoke(lambda.command('invoke'));
registerLambdaLogs(lambda.command('logs'));

const sqs = program.command('sqs')
   .description('Commands related to Amazon SQS');

registerSQSDownloadMessages(sqs.command('download-messages'));
registerSQSOldestMessageReport(sqs.command('oldest-message-report'));

const sfn = program.command('sfn') // Step Functions
   .description('Commands related to AWS Step Functions');

registerStepFunctionsListExecutions(sfn.command('list-executions'));
registerStepFunctionsStartExecutions(sfn.command('start-executions'));

registerTransform(program.command('transform'));

setUpAutocompletion(program);

program.parse();
