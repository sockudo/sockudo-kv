#!/bin/bash

# Clean up
pkill sockudo-kv
rm -rf node-*.conf node-*.log dump-*.rdb

# Build
cargo build --release

# Start 3 nodes
echo "Starting Node 7000..."
./target/release/sockudo-kv --port 7000 --cluster-enabled yes --cluster-config-file node-7000.conf --logfile node-7000.log --pidfile node-7000.pid > node-7000.out 2>&1 &
echo $! > node-7000.pid

echo "Starting Node 7001..."
./target/release/sockudo-kv --port 7001 --cluster-enabled yes --cluster-config-file node-7001.conf --logfile node-7001.log --pidfile node-7001.pid > node-7001.out 2>&1 &
echo $! > node-7001.pid

echo "Starting Node 7002..."
./target/release/sockudo-kv --port 7002 --cluster-enabled yes --cluster-config-file node-7002.conf --logfile node-7002.log --pidfile node-7002.pid > node-7002.out 2>&1 &
echo $! > node-7002.pid

sleep 2

# Assign slots
echo "Assigning slots..."
redis-cli -p 7000 CLUSTER ADDSLOTSRANGE 0 5460
redis-cli -p 7001 CLUSTER ADDSLOTSRANGE 5461 10922
redis-cli -p 7002 CLUSTER ADDSLOTSRANGE 10923 16383

# Meet
echo "Meeting..."
redis-cli -p 7000 CLUSTER MEET 127.0.0.1 7001
redis-cli -p 7000 CLUSTER MEET 127.0.0.1 7002

sleep 5

# Check nodes
echo "Checking cluster state..."
redis-cli -p 7000 CLUSTER NODES
redis-cli -p 7000 CLUSTER INFO

# Run verifier
echo "Check CLUSTER SLOTS..."
redis-cli -p 7000 CLUSTER SLOTS

echo "Testing redis-cli -c (SET)..."
redis-cli -c -p 7000 SET key value
echo "Testing redis-cli -c (GET from 7000)..."
redis-cli -c -p 7000 GET key
echo "Testing redis-cli -c (GET from 7002)..."
redis-cli -c -p 7002 GET key

# Test CLUSTER REPLICATE
echo "Waiting for gossip to converge..."
sleep 5
echo "Node 7002 view before replicate:"
redis-cli -p 7002 CLUSTER NODES

echo "Testing CLUSTER REPLICATE..."
ID_7000=$(redis-cli -p 7000 CLUSTER MYID | tr -d '[:space:]')
echo "Node 7000 ID: '$ID_7000'"
redis-cli -p 7002 CLUSTER REPLICATE $ID_7000
echo "Checking 7002 role (should be slave of $ID_7000)..."
redis-cli -p 7002 CLUSTER NODES | grep myself

# Cleanup
cat node-7000.pid | xargs kill
cat node-7001.pid | xargs kill
cat node-7002.pid | xargs kill
