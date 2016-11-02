#!/bin/bash

#set -x

if [[ $(git status --short) != "" ]]; then
   git status
   cat <<-EOF
#######################################################################
There are dirty files remaining in the AtlasDB repo after testing.
These must be cleaned up or ignored so publishing can run successfully.
^^^^^^^^^^^See the above git status to figure out the problem^^^^^^^^^^
#######################################################################
EOF
   exit 1
fi
