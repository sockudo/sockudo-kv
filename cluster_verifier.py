import redis
import time
import sys

def test_cluster():
    print("Connecting to nodes...")
    r0 = redis.Redis(host='localhost', port=7000, decode_responses=True)
    r1 = redis.Redis(host='localhost', port=7001, decode_responses=True)
    r2 = redis.Redis(host='localhost', port=7002, decode_responses=True)

    # 1. Check Gossip (CLUSTER NODES)
    # Wait for gossip to propagate
    print("Waiting for gossip...")
    success = False
    for i in range(10):
        # nodes = r0.cluster_nodes()
        # redis-py cluster_nodes returns dict or check output manually
        # Simple check: count lines in raw output
        raw_nodes = r0.execute_command("CLUSTER NODES")
        if isinstance(raw_nodes, dict):
             count = len(raw_nodes)
        else:
             count = len(raw_nodes.strip().split('\n'))
        print(f"Node 7000 sees {count} nodes")
        if count == 3:
            success = True
            break
        time.sleep(1)
    
    if not success:
        print("FAIL: Gossip did not converge (Node 0 doesn't see 3 nodes)")
        # return # Continue to check other things

    # 2. Check Redirection (MOVED)
    print("Checking Redirection...")
    # Key 'foo' (slot 12182) should be on Node 7002 (10923-16383)
    # We ask Node 7000
    try:
        r0.set("foo", "bar")
        print("FAIL: Node 7000 accepted key 'foo' (slot 12182) which belongs to 7002 without redirect/error")
    except redis.exceptions.ResponseError as e:
        err = str(e)
        if "MOVED" in err:
            print(f"SUCCESS: Received MOVED for 'foo': {err}")
            # Verify details: MOVED 12182 127.0.0.1:7002
            if "12182" in err and "7002" in err:
                 print("SUCCESS: MOVED target is correct")
            else:
                 print(f"FAIL: MOVED target incorrect: {err}")
        else:
            print(f"FAIL: Received unexpected error: {err}")
            
    # Key 'test' (slot 6918) -> Node 7001 (5461-10922)
    try:
        r0.get("test")
        print("FAIL: Node 7000 accepted key 'test' (slot 6918) which belongs to 7001")
    except redis.exceptions.ResponseError as e:
        if "MOVED" in str(e):
             print(f"SUCCESS: Received MOVED for 'test': {e}")
        else:
             print(f"FAIL: Unexpected error for 'test': {e}")

if __name__ == "__main__":
    test_cluster()
