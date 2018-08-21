#!/bin/sh

set +e

while true; do
    MEMUSAGE=$(($(ps aux | awk "{{ print \$6 }}" | tr "\n" "+"; echo 0)))
    echo "Memory usage at $(date): $MEMUSAGE"
    sleep 1
done
