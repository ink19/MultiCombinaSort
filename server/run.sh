#!/bin/bash 

for i in $(seq 0 1 $(($1 - 1))) 
do
go run server.go --data_prefix data/ --data_index $i &
done
wait