#!/bin/bash

# Install Modal
pip install -r requirements.txt

# Authenticate Modal
if [ ! -f ~/".modal.toml" ]; then
    modal token set --token-id $MODAL_TOKEN_ID --token-secret $MODAL_TOKEN_SECRET --profile=sil_ai
fi

# # Current working directory.
currentdir=$(pwd)
source .env

# Test all Modal apps.
for dir in runner assessments
do
for app in $dir/*/
do
echo ""
echo ""
echo "Deploying Test Modal app: $app"
echo "-----------------------"


cd $app
    MODAL_TEST=TRUE modal deploy app.py || exit 1   # You can add your suffix here
cd $currentdir
done
done
for dir in runner assessments
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