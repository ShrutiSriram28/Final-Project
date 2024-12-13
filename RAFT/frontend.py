import grpc
from concurrent import futures
import raft_pb2
import raft_pb2_grpc
import subprocess
import time
import os
from config import Config

class FrontEndService(raft_pb2_grpc.FrontEndServicer):
    def __init__(self):
        super().__init__()
        self.servers = {}
        self.FRONTEND_PORT = 8001
        self.BASE_SERVER_PORT = 9001
        self.leader_id = None
        self.request_ids = {}  # Add request ID tracking
        print("FrontEndService initialized")

    def get_next_request_id(self, client_id):
        """Generate next request ID for a client"""
        next_id = self.request_ids.get(client_id, 0) + 1
        self.request_ids[client_id] = next_id
        return next_id

    def StartRaft(self, request, context):
        """Start a new RAFT cluster"""
        print(f"\nStartRaft called with request: {request}")
        try:
            num_servers = request.arg
            print(f"Starting RAFT cluster with {num_servers} servers...")
            
            # Make sure no servers are running
            self._cleanup_servers()
            
            # Start servers sequentially
            for i in range(1, num_servers + 1):
                try:
                    print(f"Starting server {i}...")
                    process = subprocess.Popen(
                        ["python", "raft_server.py", str(i)],
                        stdout=open(f"raftserver{i}_output.log", 'w'),
                        stderr=open(f"raftserver{i}_error.log", 'w')
                    )
                    self.servers[i] = {
                        'process': process,
                        'port': self.BASE_SERVER_PORT + (i-1)
                    }
                    # Give each server time to initialize
                    time.sleep(0.1)  
                    
                    if process.poll() is not None:
                        error_msg = f"Server {i} failed to start"
                        print(f"Error: {error_msg}")
                        self._cleanup_servers()
                        return raft_pb2.Reply(
                            wrongLeader=True,
                            error=error_msg,
                            value=""
                        )
                        
                except Exception as e:
                    error_msg = f"Error starting server {i}: {str(e)}"
                    print(f"Error: {error_msg}")
                    self._cleanup_servers()
                    return raft_pb2.Reply(
                        wrongLeader=True,
                        error=error_msg,
                        value=""
                    )

            # Save cluster configuration
            Config.save_cluster_size(num_servers)
            
            # Wait for leader election
            success_msg = f"RAFT cluster started with {num_servers} servers"
            print(f"Success: {success_msg}")
            return raft_pb2.Reply(
                wrongLeader=False,
                error="",
                value=success_msg
            )
                
        except Exception as e:
            error_msg = f"Error in StartRaft: {str(e)}"
            print(f"Critical Error: {error_msg}")
            self._cleanup_servers()
            return raft_pb2.Reply(
                wrongLeader=True,
                error=error_msg,
                value=""
            )

    def _find_leader(self):
        """Helper method to find current leader using balanced parallel execution"""
        print("\nAttempting to find leader...")
        
        def check_server(server_info):
            server_id, info = server_info
            try:
                # Use longer timeout for initial leader election
                with grpc.insecure_channel(
                    f'localhost:{info["port"]}'
                ) as channel:
                    stub = raft_pb2_grpc.KeyValueStoreStub(channel)
                    # Give more time for response
                    response = stub.GetState(raft_pb2.Empty(), timeout=2.0)
                    if response.isLeader:
                        print(f"Found leader: server{server_id} on port {info['port']}")
                        return info["port"]
            except Exception as e:
                print(f"Error contacting server {server_id}: {str(e)}")
            return None

        # Use ThreadPoolExecutor with limit on concurrent checks
        with futures.ThreadPoolExecutor(max_workers=min(3, len(self.servers))) as executor:
            # Submit all tasks
            future_to_server = {
                executor.submit(check_server, (server_id, info)): server_id 
                for server_id, info in self.servers.items()
            }
            
            # Wait for results with timeout
            try:
                for future in futures.as_completed(future_to_server.keys(), timeout=5.0):
                    result = future.result()
                    if result is not None:
                        return result
            except TimeoutError:
                print("Leader finding timed out")
            except Exception as e:
                print(f"Error during leader finding: {e}")

        print("No leader found!")
        return None

    def Put(self, request, context):
        """Handle Put requests with proper majority failure handling"""
        client_id = request.ClientId
        request_id = self.get_next_request_id(client_id)
        print(f"KeyValueStore PUT received: key={request.key}, value={request.value}, clientId={client_id}, requestId={request_id}")
        
        modified_request = raft_pb2.KeyValue(
            key=request.key,
            value=request.value,
            ClientId=client_id,
            RequestId=request_id
        )

        # Try up to 2 complete passes through the servers
        for attempt in range(2):
            # Try leader first if known
            if self.leader_id:
                try:
                    server_info = self.servers[self.leader_id]
                    with grpc.insecure_channel(f"{Config.HOST}:{server_info['port']}") as channel:
                        stub = raft_pb2_grpc.KeyValueStoreStub(channel)
                        response = stub.Put(modified_request)
                        if response.error:  # Server returned explicit error
                            print(f"PUT failed: {response.error}")
                            return raft_pb2.Reply(
                                wrongLeader=False,
                                error=response.error,
                                value=""
                            )
                        if not response.wrongLeader:  # Successfully committed
                            return response
                except Exception as e:
                    print(f"Error contacting leader {self.leader_id}: {str(e)}")
                    self.leader_id = None

            # Try other servers
            for server_id, info in self.servers.items():
                if server_id == self.leader_id:
                    continue
                    
                try:
                    with grpc.insecure_channel(f"{Config.HOST}:{info['port']}") as channel:
                        stub = raft_pb2_grpc.KeyValueStoreStub(channel)
                        response = stub.Put(modified_request)
                        
                        if response.error:  # Server returned explicit error
                            print(f"PUT failed: {response.error}")
                            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "PUT Failed")
                            return raft_pb2.Reply(
                                wrongLeader=False,
                                error=response.error,
                                value=""
                            )
                        if not response.wrongLeader:  # Successfully committed
                            self.leader_id = server_id
                            return response
                except Exception as e:
                    print(f"Error contacting server {server_id}: {str(e)}")
                    continue

            print(f"Completed attempt {attempt+1} without finding working leader")

        # After two complete passes, return error indicating no progress possible
        return raft_pb2.Reply(
            wrongLeader=True,
            error="Failed to achieve consensus after trying all servers twice",
            value=""
        )

    def Get(self, request, context):
        """Optimized GET with proper error handling"""
        client_id = request.ClientId
        request_id = self.get_next_request_id(client_id)
        print(f"\nReceived Get request: key={request.key}, clientId={client_id}, requestId={request_id}")
        
        # Create request with request ID
        modified_request = raft_pb2.GetKey(
            key=request.key,
            ClientId=client_id,
            RequestId=request_id
        )
        
        # Try leader first if known
        if self.leader_id and self.leader_id in self.servers:
            try:
                with grpc.insecure_channel(f"{Config.HOST}:{self.servers[self.leader_id]['port']}",
                    options=[('grpc.enable_retries', 0)]) as channel:
                    stub = raft_pb2_grpc.KeyValueStoreStub(channel)
                    response = stub.Get(modified_request)
                    if not response.wrongLeader and response.value:
                        return response
            except Exception as e:
                print(f"Error contacting leader: {e}")
                self.leader_id = None
        
        # Try all servers if needed
        for server_id, info in self.servers.items():
            try:
                with grpc.insecure_channel(f"{Config.HOST}:{info['port']}") as channel:
                    stub = raft_pb2_grpc.KeyValueStoreStub(channel)
                    response = stub.Get(modified_request)
                    if not response.wrongLeader:
                        if response.value:  # Only return if we got a value
                            return response
                        else:
                            # Don't keep trying if key definitively not found
                            return raft_pb2.Reply(
                                wrongLeader=False,
                                error="Key not found",
                                value=""
                            )
            except Exception as e:
                continue
                
        return raft_pb2.Reply(
            wrongLeader=True,
            error="No available server could process the request",
            value=""
        )

    def Replace(self, request, context):
        """Forward Replace request to leader"""
        client_id = request.ClientId
        request_id = self.get_next_request_id(client_id)
        print(f"KeyValueStore REPLACE received: key={request.key}, value={request.value}, clientId={client_id}, requestId={request_id}")

        # Create new request with request ID
        modified_request = raft_pb2.KeyValue(
            key=request.key,
            value=request.value,
            ClientId=client_id,
            RequestId=request_id
        )

        if not self.servers:
            return raft_pb2.Reply(
                wrongLeader=True,
                error="No servers available",
                value=""
            )

        leader_port = self._find_leader()
        if leader_port:
            try:
                with grpc.insecure_channel(f'localhost:{leader_port}') as channel:
                    stub = raft_pb2_grpc.KeyValueStoreStub(channel)
                    response = stub.Replace(modified_request)
                    if not response.wrongLeader:
                        return response
            except Exception as e:
                print(f"Error contacting leader: {str(e)}")

        # If leader not found or request failed, try all servers
        for server_id, info in self.servers.items():
            try:
                with grpc.insecure_channel(f'localhost:{info["port"]}') as channel:
                    stub = raft_pb2_grpc.KeyValueStoreStub(channel)
                    response = stub.Replace(modified_request)
                    if not response.wrongLeader:
                        return response
            except Exception as e:
                print(f"Error contacting server {server_id}: {str(e)}")
                continue

        return raft_pb2.Reply(
            wrongLeader=True,
            error="No available server could process the request",
            value=""
        )

    def _cleanup_servers(self):
        """Clean up all running servers"""
        print("Cleaning up servers...")
        try:
            subprocess.run(["pkill", "-f", "raftserver"], check=False)
        except Exception as e:
            print(f"Error during pkill: {str(e)}")

        for server_id, server_info in self.servers.items():
            try:
                print(f"Terminating server {server_id}")
                server_info['process'].terminate()
                server_info['process'].wait(timeout=5)
            except Exception as e:
                try:
                    print(f"Killing server {server_id}")
                    server_info['process'].kill()
                    server_info['process'].wait()
                except Exception as e:
                    print(f"Error killing server {server_id}: {str(e)}")

        self.servers.clear()
        
        try:
            print("Cleaning up state files")
            subprocess.run(["rm", "-f", "state_server*.json"], check=False)
            subprocess.run(["rm", "-f", "servers.json"], check=False)
            subprocess.run(["rm", "-f", "cluster_size.json"], check=False)
        except Exception as e:
            print(f"Error cleaning state files: {str(e)}")

def serve():
    print("Starting frontend server...")
    
    try:
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        frontend_service = FrontEndService()
        
        # Add the service to the server
        raft_pb2_grpc.add_FrontEndServicer_to_server(frontend_service, server)
        
        # Print registered methods
        service = next(iter(server._state.generic_handlers))
        print("\nRegistered methods:")
        for method_name in service._method_handlers.keys():
            print(f"- {method_name}")
        
        address = f'[::]:{frontend_service.FRONTEND_PORT}'
        print(f"\nAdding insecure port: {address}")
        server.add_insecure_port(address)
        
        print("Starting server...")
        server.start()
        print(f"\nFrontend server started on port {frontend_service.FRONTEND_PORT}")
        
        try:
            while True:
                time.sleep(86400)
        except KeyboardInterrupt:
            print("\nShutting down...")
            frontend_service._cleanup_servers()
            server.stop(0)
            print("Frontend server shut down")
            
    except Exception as e:
        print(f"Error in server: {str(e)}")
        raise

if __name__ == '__main__':
    serve()