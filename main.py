#!/usr/bin/env python3
import socket, threading, time, json, uuid, sys, math, random

BCAST_PORT = 50000
BCAST_ADDR = "<broadcast>"
HELLO_INTERVAL = 1.0
PEER_TTL = 4.0  # seconds before a peer is considered dead

JOB_EVERY = 10.0
JOB_N = 3_000_000  # total numbers [0, N) to sum squares over (adjust if slow)

node_id = str(uuid.uuid4())
peers = {}  # node_id -> {'addr': (ip, tcp_port), 'last': time.time()}
peers_lock = threading.Lock()

# --- TCP worker server (handles jobs) ----------------------------------------
def sum_squares(start, end):
    # sum of k^2, start <= k < end (chunked to avoid Python sum overhead)
    # use formula for speed, but keep chunk behavior to show "distributed" work:
    def f(n): return n*(n+1)*(2*n+1)//6
    # sum_{k=start}^{end-1} k^2 = f(end-1) - f(start-1)
    if start == 0:
        return f(end-1)
    return f(end-1) - f(start-1)

def handle_conn(conn, addr):
    try:
        buf = b""
        while b"\n" not in buf:
            chunk = conn.recv(4096)
            if not chunk: return
            buf += chunk
        msg = json.loads(buf.split(b"\n",1)[0].decode("utf-8"))
        if msg.get("type") == "job":
            start = int(msg["start"]); end = int(msg["end"])
            res = int(sum_squares(start, end))
            reply = {"type":"result","job_id":msg["job_id"],"result":res}
            conn.sendall((json.dumps(reply)+"\n").encode("utf-8"))
    finally:
        conn.close()

def tcp_server(tcp_port_holder):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))  # pick free port
    s.listen()
    tcp_port = s.getsockname()[1]
    tcp_port_holder.append(tcp_port)
    while True:
        conn, addr = s.accept()
        threading.Thread(target=handle_conn, args=(conn, addr), daemon=True).start()

# --- UDP discovery (broadcast) -----------------------------------------------
def udp_hello_sender(get_tcp_port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.settimeout(1.0)
    while True:
        msg = {"type":"hello","id":node_id,"tcp_port":get_tcp_port()}
        data = (json.dumps(msg)+"\n").encode("utf-8")
        try:
            sock.sendto(data, (BCAST_ADDR, BCAST_PORT))
        except Exception:
            pass
        time.sleep(HELLO_INTERVAL)

def udp_hello_listener():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # On some UNIXes, binding to '' with SO_REUSEADDR lets multiple binders coexist for bcast.
    sock.bind(("", BCAST_PORT))
    while True:
        data, (ip, _) = sock.recvfrom(4096)
        try:
            msg = json.loads(data.decode("utf-8"))
        except Exception:
            continue
        if msg.get("type") != "hello": 
            continue
        peer_id = msg.get("id")
        if not peer_id or peer_id == node_id:
            continue
        with peers_lock:
            peers[peer_id] = {"addr": (ip, int(msg.get("tcp_port", 0))), "last": time.time()}

def prune_peers_loop():
    while True:
        now = time.time()
        with peers_lock:
            dead = [pid for pid,info in peers.items() if (now - info["last"]) > PEER_TTL]
            for d in dead: peers.pop(d, None)
        time.sleep(1.0)

# --- Leader election & job distribution --------------------------------------
def current_cluster():
    with peers_lock:
        # include self
        all_nodes = {node_id: {"addr": ("127.0.0.1", tcp_port_holder[0]), "last": time.time()}}
        all_nodes.update(peers)
        return all_nodes

def is_leader():
    # Leader = lexicographically smallest node_id
    ids = sorted(current_cluster().keys())
    return ids and ids[0] == node_id

def split_ranges(n, k):
    k = max(1, k)
    base = n // k
    rem = n % k
    ranges = []
    start = 0
    for i in range(k):
        sz = base + (1 if i < rem else 0)
        end = start + sz
        ranges.append((start, end))
        start = end
    return ranges

def send_job(ip, port, job_id, start, end):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5.0)
    s.connect((ip, port))
    s.sendall((json.dumps({"type":"job","job_id":job_id,"start":start,"end":end})+"\n").encode("utf-8"))
    # read one line
    buf = b""
    while b"\n" not in buf:
        chunk = s.recv(4096)
        if not chunk: raise RuntimeError("connection closed")
        buf += chunk
    s.close()
    msg = json.loads(buf.split(b"\n",1)[0].decode("utf-8"))
    return int(msg["result"])

def leader_loop():
    last_run = 0.0
    while True:
        time.sleep(0.5)
        if not is_leader():
            continue
        if time.time() - last_run < JOB_EVERY:
            continue
        nodes = current_cluster()
        # Require at least 1 peer (or run anyway with just self)
        k = len(nodes)
        if k == 0:
            continue
        print(f"[leader {node_id[:8]}] peers={k} -> distributing job N={JOB_N}")
        job_id = str(uuid.uuid4())
        chunks = split_ranges(JOB_N, k)
        # Map node list to ranges in a stable order
        node_items = sorted(nodes.items(), key=lambda x: x[0])
        results = []
        for (pid, info), (a,b) in zip(node_items, chunks):
            ip, port = info["addr"]
            if pid == node_id:
                # compute locally without TCP for speed
                r = sum_squares(a, b)
            else:
                r = send_job(ip, port, job_id, a, b)
            results.append(r)
            print(f"  chunk {a}:{b} from {pid[:8]} -> {r}")
        total = sum(results)
        # Verification against formula:
        formula = (JOB_N-1)*JOB_N*(2*JOB_N-1)//6 if JOB_N>0 else 0
        ok = "OK" if total == formula else f"MISMATCH (expected {formula})"
        print(f"[leader] total={total}  {ok}\n")
        last_run = time.time()

# --- main ---------------------------------------------------------------------
def main():
    print(f"node_id={node_id}")
    tcp_port_holder = []
    threading.Thread(target=tcp_server, args=(tcp_port_holder,), daemon=True).start()
    # Wait until TCP port known
    while not tcp_port_holder: time.sleep(0.01)
    print(f"tcp_port={tcp_port_holder[0]}")
    threading.Thread(target=udp_hello_sender, args=(lambda: tcp_port_holder[0],), daemon=True).start()
    threading.Thread(target=udp_hello_listener, daemon=True).start()
    threading.Thread(target=prune_peers_loop, daemon=True).start()
    threading.Thread(target=leader_loop, daemon=True).start()
    # Keep main alive
    try:
        while True: time.sleep(3600)
    except KeyboardInterrupt:
        sys.exit(0)    

if __name__ == "__main__":
    main()