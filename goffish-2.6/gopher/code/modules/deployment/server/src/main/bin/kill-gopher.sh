#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
for p in `cat $DIR/kill_list`
do
	echo $p
	kill -9 $p
done

rm $DIR/kill_list
