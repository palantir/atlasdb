#!/bin/bash

set -x

if command -v python &>/dev/null; then
    echo "Python is present."
else
    echo "Python not found, installing."
    sudo apt-get install python
fi

python -c "import dateutil" &>/dev/null
dateutilAbsent=$?

if [[ ${dateutilAbsent} -ne 0 ]]; then
    echo "DateUtil not found, installing."
    sudo apt-get install python-dateutil
else
    echo "DateUtil is present."
fi
