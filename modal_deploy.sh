#!/bin/bash

# Current working directory.
currentdir=$(pwd)

# Deploy all Modal apps.
for dir in runner assessments
do 
for app in $dir/*/
do
echo ""
echo ""
echo "Deploying Modal app: $app"
echo "-----------------------"
    cd $app
    modal deploy app.py || exit 1
    cd $currentdir
done
done