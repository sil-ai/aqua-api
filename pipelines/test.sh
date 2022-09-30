#!/bin/bash

# Current working directory.
currentdir=$(pwd)

# Test pipeline scripts.
for dir in scripts/*/
do
    echo ""
    echo "Testing $dir"
    echo "-----------------------"
    cd $dir
    make test || exit 1
    cd $currentdir
done