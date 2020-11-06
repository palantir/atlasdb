#!/bin/bash

set -x -e

MOREUTILS="moreutils_0.64-1_amd64.deb"
curl --fail -O "http://http.us.debian.org/debian/pool/main/m/moreutils/$MOREUTILS"
ar p $MOREUTILS data.tar.xz | sudo tar xJ --strip-components=3 -C /usr/bin/ ./usr/bin/ts
rm $MOREUTILS
