#!/bin/bash

set -o pipefail

"$@" 2>&1 | ts -s "[%H:%M:%S]"
