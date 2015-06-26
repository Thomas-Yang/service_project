#!/bin/bash

# Helper script to generate thrift files

rm -rf ./gen-cpp 

for file in $(ls |grep thrift)
do
	thrift --gen cpp $file
done

#thrift --gen cpp service.thrift
