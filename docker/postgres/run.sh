#!/bin/bash

for file in "/bdjuno/database/schema"/*
do
  
  psql -U postgres -c -d sesame -f $file
done