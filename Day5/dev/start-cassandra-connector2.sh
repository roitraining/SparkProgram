#! /bin/sh
curl -X POST -H “Content-Type: application/json” -d @connect-cassandra-source.json localhost:8083/connectors

