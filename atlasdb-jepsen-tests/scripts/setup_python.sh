#!/bin/bash

#
# (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -x

# Create a virtual environment in the project directory
python3 -m venv .venv

# Activate the virtual environment
source .venv/bin/activate

# Install the required Python packages in the virtual environment
pip install python-dateutil

# Verify installation
if python -c "import dateutil" >/dev/null 2>&1; then
    echo "DateUtil is present."
else
    echo "DateUtil installation failed."
    exit 1
fi

# Deactivate the virtual environment
deactivate