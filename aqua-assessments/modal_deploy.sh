#!/bin/bash

# Current working directory.
currentdir=$(pwd)

# Deploy all Modal apps.
for dir in runner assessments
do 
for app in $dir/*/
do
if [[ "$app" == *"question-answering"* ]] || [[ "$app" == *"triangulation"* ]] || [[ "$app" == *"word_tests"* ]]; then
            echo "Skipping directory: $app"
            continue # Skip the rest of the loop for this iteration
fi
echo ""
echo ""
echo "Deploying Modal app: $app"
echo "-----------------------"
    cd $app
    modal deploy app.py || exit 1
    cd $currentdir
done
done