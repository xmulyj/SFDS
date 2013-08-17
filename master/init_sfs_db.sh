#!/bin/bash

MYSQL="mysql -uroot -p888888"

$MYSQL -e "create database if not exists SFS;"

index=`seq 0 255`
for i in $index;do
	sub=`printf "%02X" $i`
	#echo $sub
	$MYSQL -e "create table if not exists SFS.fileinfo_$sub (fid varchar(40) not null, name varchar(128), size int not null, chunkid varchar(10) not null, chunkip varchar(10) not null, chunkport int not null, findex int not null, foffset int not null, primary key(fid, chunkid));"
done 
