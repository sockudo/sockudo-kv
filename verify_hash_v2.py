import socket
import sys

def send_command(sock, command):
    parts = command.split()
    buf = f"*{len(parts)}\r\n"
    for part in parts:
        buf += f"${len(part)}\r\n{part}\r\n"
    sock.sendall(buf.encode())
    
    # Simple RESP reader (very basic)
    response = sock.recv(4096).decode()
    return response

def test():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("127.0.0.1", 6379))
        
        print("1. HSET myhash f1 v1 f2 v2 f3 v3")
        print(send_command(s, "HSET myhash f1 v1 f2 v2 f3 v3"))
        
        print("2. HSCAN myhash 0")
        scan_res = send_command(s, "HSCAN myhash 0")
        print(scan_res)
        if "f1" not in scan_res or "v2" not in scan_res:
            print("FAILURE: HSCAN missing fields")
            sys.exit(1)

        print("3. HSCAN myhash 0 NOVALUES")
        scan_res_nv = send_command(s, "HSCAN myhash 0 NOVALUES")
        print(scan_res_nv)
        if "v1" in scan_res_nv or "v2" in scan_res_nv:
             print("FAILURE: HSCAN NOVALUES returned values")
             sys.exit(1)
        if "f1" not in scan_res_nv:
             print("FAILURE: HSCAN NOVALUES missing keys")
             sys.exit(1)
            
        print("4. HGETDEL myhash FIELDS 2 f1 f2")
        val = send_command(s, "HGETDEL myhash FIELDS 2 f1 f2")
        print(val)
        if "v1" not in val or "v2" not in val:
            print("FAILURE: HGETDEL did not return v1/v2")
            sys.exit(1)
        if "v3" in val:
            print("FAILURE: HGETDEL returned v3 (unrequested)")
            sys.exit(1)

        print("5. HGET myhash f1 (should be nil)")
        val = send_command(s, "HGET myhash f1")
        print(val)
        if "$-1" not in val:
            print("FAILURE: HGET f1 still exists")
            sys.exit(1)

        print("6. HSCAN myhash 0 (should only have f3)")
        scan_res = send_command(s, "HSCAN myhash 0")
        print(scan_res)
        if "f1" in scan_res or "f2" in scan_res:
             print("FAILURE: f1/f2 still in scan")
             sys.exit(1)
        if "f3" not in scan_res:
             print("FAILURE: f3 missing")
             sys.exit(1)

        print("SUCCESS")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        s.close()

if __name__ == "__main__":
    test()
