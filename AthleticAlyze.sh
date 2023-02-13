#!/bin/bash

indexFolder="AthleticAlyzeIndex"
if [ -d "$indexFolder" ] ; then
        rm -rf "AthleticAlyzeIndex"
fi
pythonFile="./AthleticAlyze.py"
csvFile="$1"
searchKey="$2"
searchCol="$3"

python3 $pythonFile "$csvFile" "$searchKey" "$searchCol"