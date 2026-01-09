#!/bin/bash
# Expects running cluster from cluster_test.sh

# {user1} maps to slot ...
# {user2} maps to slot ...
# We need two keys that map to DIFFERENT slots but one is owned by 7000 and one by 7001 (or different slots on 7000? No, Redis Cluster requires same slot).
# Redis Cluster requires ALL keys in a command to hash to the EXACT SAME SLOT.

# Let's find two keys that hash to different slots.
# key "a" -> slot ?
# key "b" -> slot ?

echo "Testing CROSSSLOT behavior..."

# We need to send this to Node 7000.
# We need the FIRST key to map to a slot owned by 7000 (so it passes the first check).
# We need the SECOND key to map to a DIFFERENT slot (so it SHOULD fail with CROSSSLOT).

# I'll use python to find such keys if needed, or just guess.
# 7000 owns 0-5460.
# "a" (slot 15495) -> Node 7002
# "foo" (slot 12182) -> Node 7002
# "test" (slot 6918) -> Node 7001
# "key" (slot 12539) -> Node 7002

# Need a key for slot < 5460.
# "1" -> 9842
# "aa" -> 1180
# "bb" -> 7815
# "cc" -> 15306
# "dd" -> 6344
# "ee" -> 2327 -> Owned by 7000.

# So "ee" is on 7000.
# "test" is on 7001.

# Command: MSET ee val1 test val2
# Expected: CROSSSLOT error (because 'ee' and 'test' are different slots).
# This test verifies the fix.

redis-cli -p 7000 MSET ee val1 test val2
redis-cli -p 7000 GET ee
redis-cli -p 7000 GET test
# 'test' should be on 7001. If 7000 has it, we have data corruption/split brain of data.

echo "Checking if 7001 has 'test'..."
redis-cli -p 7001 GET test
