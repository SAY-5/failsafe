#!/bin/sh
set -e
cd "$(dirname "$0")"
./node_modules/.bin/tsc -b
./node_modules/.bin/vite build
