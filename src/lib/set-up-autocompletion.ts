import { Command } from 'commander';
import omelette from 'omelette';
import { isNotNullOrUndefined } from '@silvermine/toolbox';

function mapAutocompleteTree(command: Command, prefix?: string): Record<string, string[]> {
   const path = (prefix ? prefix + ' ' : '') + command.name();

   const choices = command.options
      .map((option) => {
         return option.long;
      }, [])
      .filter(isNotNullOrUndefined);

   const tree = command.commands.reduce((memo: Record<string, string[]>, subcommand) => {
      choices.push(subcommand.name());

      return {
         ...memo,
         ...mapAutocompleteTree(subcommand, path),
      };
   }, {});

   tree[path] = choices;

   return tree;
}

export default function setUpAutocompletion(program: Command): void {
   const completion = omelette(program.name()),
         autocompleteTree = mapAutocompleteTree(program);

   completion.on('complete', (_, { line, reply }) => {
      const path = line.replace(/ -{1,2}[^ ]+/g, '').trim(),
            choices = autocompleteTree[path];

      if (choices) {
         reply(choices);
      }
   });

   completion.init();

   if (process.argv.includes('--install-autocompletion')) {
      completion.setupShellInitFile();
   }
}
