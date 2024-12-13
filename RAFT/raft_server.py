from concurrent.futures import ThreadPoolExecutor
import os
import sys
from config import Config
from node import RaftNode
import time
import setproctitle


def main():
    if len(sys.argv) != 2:
        print("Usage: python raft_server.py <node_id>")
        sys.exit(1)

    server_num = int(sys.argv[1])
    # Set process name for pkill testing
    setproctitle.setproctitle(f"raftserver{server_num}")

    node = RaftNode(f"server{server_num}")
    node_id = f"server{server_num}"
    
    try:
        node.start()
        # Keep the main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down node...")
        
        def stop_server():
            node.server.stop(0)
            print("gRPC server stopped")
            
        def remove_from_config():
            servers = Config.load_servers()
            if node_id in servers:
                del servers[node_id]
                Config.save_servers(servers)
                print(f"Removed {node_id} from cluster configuration")
        
        # Execute both tasks concurrently
        with ThreadPoolExecutor(max_workers=2) as executor:
            stop_future = executor.submit(stop_server)
            remove_future = executor.submit(remove_from_config)
            
            # Wait for both tasks to complete
            stop_future.result()
            remove_future.result()
            
        print(f"Node {node_id} successfully shut down")
        sys.exit(0)

if __name__ == "__main__":
    main()