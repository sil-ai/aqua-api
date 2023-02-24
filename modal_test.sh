#!/bin/bash

# Install Modal
pip install modal-client pytest

# # Authenticate Modal
modal token set --token-id $MODAL_TOKEN_ID --token-secret $MODAL_TOKEN_SECRET --env=sil-ai

# # Current working directory.
currentdir=$(pwd)

# # Deploy dummy assessment endpoint (in case it was updated)
modal deploy assessments/dummy/app.py || exit 1

# Test all Modal apps.
for dir in assessments runner
do 
for app in $dir/*/
do
echo ""
echo ""
echo "Deploying Test Modal app: $app"
echo "-----------------------"
    cd $app
    MODAL_TEST=TRUE modal deploy app.py || exit 1
    cd $currentdir
done
done
for dir in assessments runner
do 
for app in $dir/*/
do
echo ""
echo ""
echo "Testing Modal app: $app"
echo "-----------------------"
    cd $app
    pytest || exit 1
    cd $currentdir
done
done