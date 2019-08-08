#!/usr/bin/env bash
set -euo pipefail

main() {
    echo "Recording 'ps' info every second to ~/artifacts/monitor_all_processes.log..."
    while true ; do
      JSON=$(ps --no-headers -e --format pid,rss,vsz,cmd | jq -c --raw-input --slurp 'split("\n") | map(select(. != ""))')
      TIME=$(date --iso-8601=ns)

      echo '{ "time": "'"$TIME"'", "ps": '"$JSON"' }' >> ~/artifacts/monitor_all_processes.log
      sleep 1
    done
}

usage() {
cat << EndOfMessage
usage: monitor_all_processes [--help|--version]

Record 'ps' info every second to ~/artifacts/monitor_all_processes.log.

Gradle and Java processes are notorious for taking up unpredictable amounts of
heap. This script is intended to run as a background step of each CircleCI
job, e.g.:

    - run: { command: monitor_all_processes, background: true }

A separate step should ensure the ~/artifacts/monitor_all_processes.log file is
collected, e.g.:

    - store_artifacts: { path: ~/artifacts }

Each line of monitor_all_processes.log has the following JSON structure:

    {
      "time": "2019-04-01T15:15:50,039477157+00:00",
      "ps": [
        "   87 86264 3236000 /opt/java/jdk-11.0.2/bin/java -Xmx64m -Xms64m...",
        ...
      ]
    }

Each string in the 'ps' array shows:

                87: the process id
             86264: rss (resident set size) in kilobytes
           3236000: vsz (virtual set size) in kilobytes
  /opt/java/jdk...: the actual command that was run

To work with a monitor_all_processes.log file, it is recommended to pretty-print the entire document.
EndOfMessage
}

if [ -z "${1:+x}" ]; then
    main
elif [ "$1" = "--version" ]; then
    echo "1.0.0"
elif [ "$1" = "--help" ]; then
    usage
else
    usage
    exit 1
fi