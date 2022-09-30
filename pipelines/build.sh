#!/bin/bash

# Current working directory.
currentdir=$(pwd)

# Build pipeline scripts.
for dir in scripts/*/
do
    echo ""
    echo "Building $dir"
    echo "-----------------------"
    cd $dir
    make build-actions || exit 1
    cd $currentdir
done