#!/bin/bash

# Current working directory.
currentdir=$(pwd)

# Push pipeline script images.
for dir in scripts/*/
do
    echo ""
    echo "Pushing $dir"
    echo "-----------------------"
    cd $dir
    make push-branch || exit 1
    cd $currentdir
done
