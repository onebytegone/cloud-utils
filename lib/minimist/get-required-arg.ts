export function getRequiredArg<T extends Record<string, unknown>, U extends keyof T>(argv: T, prop: U): T[U] {
   if (argv[prop]) {
      return argv[prop];
   }

   throw new Error(`Argument --${String(prop)} is required`);
}
