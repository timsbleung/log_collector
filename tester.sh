#!/bin/bash
rm conf/*.conf
echo "NEW RUN"
echo $(($(date +%s%N)/1000000))
touch $1
sleep 10s

