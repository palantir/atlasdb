#!/bin/bash

set -x -e

MOREUTILS="moreutils_0.60-1_amd64.deb"

curl -O "http://snapshot.debian.org/archive/debian/20170123T091837Z/pool/main/m/moreutils/$MOREUTILS"
ar p $MOREUTILS data.tar.xz | sudo tar xJ --strip-components=3 -C /usr/bin/ ./usr/bin/ts
rm $MOREUTILS
