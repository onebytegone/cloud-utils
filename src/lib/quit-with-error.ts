export function quitWithError(error: string): never {
   console.error(error);
   process.exit(1);
}
