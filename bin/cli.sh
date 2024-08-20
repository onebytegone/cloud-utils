#!/usr/bin/env bash

SCRIPT_DIR="$( cd "$( dirname $( realpath "${BASH_SOURCE[0]}" ) )" && pwd )"

"${SCRIPT_DIR}/../node_modules/.bin/ts-node" --transpile-only "${SCRIPT_DIR}/../src/cli.ts" "$@"
