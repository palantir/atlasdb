#!/bin/bash

set -e

echo This will overwrite any existing repository git hooks you currently have
read -n 1 -r -p "Do you want to continue? " choice
echo

case "$choice" in
    y|Y)
        echo "Installing hooks"
        rm -rf "$(git rev-parse --show-toplevel)/.git/hooks"
        ln -s "../scripts/hooks" "$(git rev-parse --show-toplevel)/.git/hooks"
        ;;
    *)
        echo "Exiting..."
esac
