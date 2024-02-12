import { Command } from 'commander';
import { version } from '../package.json';
import registerSQSDownloadMessages from './commands/sqs/download-messages';

const program = new Command();

program.name('cloud-utils');
program.version(version)

const sqs = program.command('sqs')
   .description('Commands related to Amazon SQS');

registerSQSDownloadMessages(sqs.command('download-messages'));

program.parse();
