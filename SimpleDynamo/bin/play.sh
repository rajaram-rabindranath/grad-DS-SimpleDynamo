#!/bin/bash
echo "phase" $1 "running"
./logcat_clear.sh
./simpledynamo-grading.linux -n 5 -p $1 SimpleDynamo.apk
