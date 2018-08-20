#!/bin/bash

set -x

if command -v python >/dev/null 2>&1; then
    echo "Python is present."
else
    echo "Python not found, installing."
    sudo apt-get install python
fi

if python -c "import dateutil" >/dev/null 2>&1; then
    echo "DateUtil is present."
else
    echo "DateUtil not found. Installing."
    sudo apt-get install python-dateutil
fi
