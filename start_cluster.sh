#!/bin/bash
# Clean
pkill sockudo-kv
rm -rf node-*.conf node-*.log dump-*.rdb

# Build
cargo build --release

# Start
./target/release/sockudo-kv --port 7000 --cluster-enabled yes --cluster-config-file node-7000.conf --logfile node-7000.log --pidfile node-7000.pid > node-7000.out 2>&1 &
./target/release/sockudo-kv --port 7001 --cluster-enabled yes --cluster-config-file node-7001.conf --logfile node-7001.log --pidfile node-7001.pid > node-7001.out 2>&1 &
./target/release/sockudo-kv --port 7002 --cluster-enabled yes --cluster-config-file node-7002.conf --logfile node-7002.log --pidfile node-7002.pid > node-7002.out 2>&1 &

sleep 2

# Assign
redis-cli -p 7000 CLUSTER ADDSLOTSRANGE 0 5460
redis-cli -p 7001 CLUSTER ADDSLOTSRANGE 5461 10922
redis-cli -p 7002 CLUSTER ADDSLOTSRANGE 10923 16383

# Meet
redis-cli -p 7000 CLUSTER MEET 127.0.0.1 7001
redis-cli -p 7000 CLUSTER MEET 127.0.0.1 7002

sleep 2
echo "Cluster started."
