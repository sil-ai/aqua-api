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
    make push || exit 1
    cd $currentdir
done