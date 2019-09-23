#! /bin/sh
mysql -ppassword -e "drop database metastore;"
mysql -ppassword -e "create database metastore;"
schematool -initSchema -dbType mysql

