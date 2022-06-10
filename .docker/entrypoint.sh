#!/bin/bash

echo 'python3 -m ddbms_chat.phase3.run' >> ~/.bashrc

# call base image's entrypoint
/usr/local/bin/docker-entrypoint.sh mariadbd &

until mysqladmin ping -h localhost -u$MARIADB_USER -p$MARIADB_PASSWORD; do
    sleep 1
    echo Waiting for db to come up...
done

echo Waiting for a long time to ensure other dbs are also up...
sleep 5 # increase this if it doesnt work on your system

if [[ -n $DO_DB_INIT ]]; then
    python3 -m ddbms_chat.phase2.syscat
    python3 -m ddbms_chat.phase2.app_tables
fi

echo Waiting for a long time to ensure system catalog is initialized...
sleep 5 # increase this if it doesnt work on your system

python3 -m ddbms_chat.phase3.daemon
