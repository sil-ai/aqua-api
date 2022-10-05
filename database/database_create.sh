#!/bin/bash

hasura init api-migration --endpoint $1 --admin-secret $2

cd api-migration
hasura deploy --endpoint $3 --admin-secret $4
cd ..
rm -rf api-migration
