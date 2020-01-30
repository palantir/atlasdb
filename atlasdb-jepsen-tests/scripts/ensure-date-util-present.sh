#!/bin/bash

set -x

if command -v python3 >/dev/null 2>&1; then
    echo "Python is present."
else
    echo "Python not found, installing."
    sudo apt-get install python3
fi

if python3 -c "import dateutil" >/dev/null 2>&1; then
    echo "DateUtil is present."
else
    echo "DateUtil not found. Installing."
    sudo pip3 install python-dateutil
fi
