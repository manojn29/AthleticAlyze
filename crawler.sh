#!/bin/bash

fileName="./crawler.py"
pfile="$1"
maxTweets="$2"
python3 $fileName "$pfile" "$maxTweets"