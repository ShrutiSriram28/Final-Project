import time
import grpc
from concurrent import futures
import threading
from random import randint
import raft_pb2
import raft_pb2_grpc
from config import Config, NodeState
from state_manager import StateManager
from concurrent.futures import ThreadPoolExecutor
import json
import socket
from typing import Dict

class ConnectionManager:
    def __init__(self, server_id: int, total_servers: int):
        self.server_id = server_id
        self.total_servers = total_servers
        self.connections: Dict[int, grpc.Channel] = {}
        self.lock = threading.Lock()
        
    def _calculate_source_port(self, target_server_id: int) -> int:
        """
        Calculate source port based on server mapping requirements:
        Server N should use ports starting from 7000 + (N-1)*5 + 1
        For example:
        - Server 1: 7001-7005
        - Server 2: 7006-7010
        - Server 3: 7011-7015
        - Server 4: 7016-7020
        - Server 5: 7021-7025
        """
        base = 7000 + (self.server_id - 1) * self.total_servers
        offset = 1  # Start from base + 1
        
        # Skip the port corresponding to self.server_id
        target_offset = target_server_id
        if target_server_id > self.server_id:
            target_offset -= 1
            
        return base + target_offset
        
    def get_channel(self, target_server_id: int) -> grpc.Channel:
        """Get or create a channel to target server with specific source port"""
        with self.lock:
            if target_server_id not in self.connections:
                target_port = 9000 + target_server_id
                source_port = self._calculate_source_port(target_server_id)
                
                print(f"Creating connection from server{self.server_id} to server{target_server_id}")
                print(f"Source port: {source_port}, Target port: {target_port}")

                try:
                    # Create socket with specific source port
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    sock.bind(('localhost', source_port))
                    
                    # Connect to target
                    sock.connect(('localhost', target_port))
                    
                    # Create channel using the connected socket
                    channel = grpc.insecure_channel(
                        f'localhost:{target_port}',
                        options = [
                        ('grpc.enable_retries', 1),
                        ('grpc.keepalive_time_ms', 10000),  # Send keepalive every 10s
                        ('grpc.keepalive_timeout_ms', 5000),  # Wait up to 5s for keepalive response
                        ('grpc.keepalive_permit_without_calls', 1),
                        ('grpc.http2.min_time_between_pings_ms', 10000),  # Minimum 10s between pings
                        ('grpc.http2.max_pings_without_data', 0),  # Allow pings without data
                        ('grpc.max_receive_message_length', -1),  # Unlimited message size
                        ('grpc.max_send_message_length', -1)      # Unlimited message size
                    ]
                    )
                    
                    # Test the connection
                    self.connections[target_server_id] = channel
                    
                except Exception as e:
                    print(f"Channel creation failed for server{target_server_id}: {e}")
                    if channel:
                        channel.close()
                    raise
                        
                except Exception as e:
                    print(f"Socket connection failed to server{target_server_id}: {e}")
                    raise
                    
            return self.connections[target_server_id]
    
    def close_all(self):
        """Close all connections"""
        with self.lock:
            for channel in self.connections.values():
                try:
                    channel.close()
                except:
                    pass
            self.connections.clear()

class KeyValueStoreService(raft_pb2_grpc.KeyValueStoreServicer):
    def __init__(self, raft_node):
        self.raft_node = raft_node

    def _convert_response(self, send_command_response):
        """Convert SendCommandResponse to Reply format"""
        print(f"Converting response: {send_command_response}")
        
        # For leader-related failures
        if not send_command_response.success and send_command_response.message.startswith("Failed to forward"):
            return raft_pb2.Reply(
                wrongLeader=True,
                error=send_command_response.message,
                value=""
            )
        
        # For operation-specific failures
        if not send_command_response.success:
            return raft_pb2.Reply(
                wrongLeader=False,
                error=send_command_response.message,
                value=""
            )
        
        # For other successful operations
        print(f"Operation successful: {send_command_response.message}")
        return raft_pb2.Reply(
            wrongLeader=False,
            error="",
            value=send_command_response.message
        )

    def GetState(self, request, context):
        """Get current node state"""
        print(f"\nGetState called on {self.raft_node.node_id}")
        print(f"Current state: {self.raft_node.current_state}")
        print(f"Current term: {self.raft_node.state_manager.get_current_term()}")
        print(f"Current leader: {self.raft_node.leader_id}")
        
        # Only return isLeader=True if we are definitely the current leader
        is_leader = (self.raft_node.current_state == NodeState.LEADER)
        
        return raft_pb2.State(
            term=self.raft_node.state_manager.get_current_term(),
            isLeader=is_leader
        )

    def Get(self, request, context):
        """Handle Get requests using existing SendCommand"""
        print(f"KeyValueStore GET received: key={request.key}, clientId={request.ClientId}")
        command = f"GET {request.key}"
        response = self.raft_node.SendCommand(
            raft_pb2.SendCommandRequest(
                msg=command,
                client_id=request.ClientId,
                request_id=request.RequestId
            ),
            context
        )
        print(f"GET response: {response}")
        return self._convert_response(response)

    def Put(self, request, context):
        """Handle Put requests using existing SendCommand"""
        print(f"KeyValueStore PUT received: key={request.key}, value={request.value}, clientId={request.ClientId}")
        command = f"PUT {request.key} {request.value}"
        response = self.raft_node.SendCommand(
            raft_pb2.SendCommandRequest(
                msg=command,
                client_id=request.ClientId,
                request_id=request.RequestId
            ),
            context
        )
        print(f"PUT response: {response}")
        return self._convert_response(response)

    def Replace(self, request, context):
        """Handle Replace requests using existing SendCommand"""
        print(f"KeyValueStore REPLACE received: key={request.key}, value={request.value}, clientId={request.ClientId}")
        command = f"REPLACE {request.key} {request.value}"
        response = self.raft_node.SendCommand(
            raft_pb2.SendCommandRequest(
                msg=command,
                client_id=request.ClientId,
                request_id=request.RequestId
            ),
            context
        )
        print(f"REPLACE response: {response}")
        return self._convert_response(response)

class RaftNode(raft_pb2_grpc.RaftNodeServicer):
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.server_num = int(node_id.replace('server', ''))
        self.state_manager = StateManager(node_id)
        self.current_state = NodeState.FOLLOWER
        self.leader_id = None
        
        # Node configuration
        self.port = 9000 + self.server_num
        
        # Initialize connection manager
        cluster_size = Config.load_cluster_size() or len(Config.load_servers())
        self.connection_manager = ConnectionManager(self.server_num, cluster_size)
        
        # Election variables
        self.election_timeout = randint(Config.MIN_ELECTION_TIMEOUT, Config.MAX_ELECTION_TIMEOUT) / 1000
        self.last_heartbeat = time.time()
        self.votes_received = 0
        
        # Initialize server
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        raft_pb2_grpc.add_RaftNodeServicer_to_server(self, self.server)

        # Add the KeyValueStore service
        kvstore_service = KeyValueStoreService(self)
        raft_pb2_grpc.add_KeyValueStoreServicer_to_server(kvstore_service, self.server)
        
        self.server.add_insecure_port(f'{Config.HOST}:{self.port}')
        
        # Start election timer
        self.election_timer = threading.Thread(target=self._run_election_timer)
        self.election_timer.daemon = True
        self.election_flag = False
        
        self.heartbeat_thread = threading.Thread(target=self._send_heartbeats)
        self.heartbeat_thread.daemon = True

    def _get_stub_for_server(self, server_id: str) -> raft_pb2_grpc.RaftNodeStub:
        """Get gRPC stub for communicating with a specific server"""
        target_num = int(server_id.replace('server', ''))
        channel = self.connection_manager.get_channel(target_num)
        return raft_pb2_grpc.RaftNodeStub(channel)

    def register_server(self):
        """Register this server in the servers.json file"""
        # Load existing servers
        servers = Config.load_servers()
        
        # Add/update this server's information
        servers[self.node_id] = {
            "port": self.port,
            "address": "localhost:" + str(self.port)
        }
        
        # Save updated server list
        Config.save_servers(servers)
        print(f"Registered {self.node_id} in {Config.SERVERS_FILE}")

    def start(self):
        """Start the RAFT node"""
        print(f"\nStarting node {self.node_id}")
        print(f"Initial state: {self.current_state}")
        print(f"Port: {self.port}")
        
        self.register_server()

        self.server.start()
        print("gRPC server started")
        
        print("Starting election timer thread")
        self.election_timer.start()
        print("Election timer thread started")
    
        while True:
            time.sleep(1)

    def _run_election_timer(self):
        """Monitor for election timeout"""
        while True:
            time.sleep(0.1)  
            
            if self.current_state == NodeState.LEADER:
                continue

            time_since_last_heartbeat = time.time() - self.last_heartbeat
            
            if self.current_state == NodeState.FOLLOWER:
                if time_since_last_heartbeat > self.election_timeout:
                    print(f"\n{self.node_id} TIMEOUT: {time_since_last_heartbeat:.2f}s > {self.election_timeout:.2f}s")
                    self._start_election()

    def _has_quorum_available(self):
        """Check if enough servers are reachable for quorum"""
        servers = Config.load_servers()
        cluster_size = Config.load_cluster_size() or len(servers)
        majority_needed = (cluster_size // 2) + 1
        reachable_servers = 0  # Start at 0, we'll count self only once
        
        def check_server_reachable(server_id):
            if server_id == self.node_id:
                return True  # We'll count self
            try:
                stub = self._get_stub_for_server(server_id)
                # Important: use empty AppendEntries as heartbeat
                response = stub.AppendEntries(raft_pb2.AppendEntriesRequest(
                    term=self.state_manager.get_current_term(),
                    leader_id=self.node_id,
                    prev_log_index=-1,
                    prev_log_term=0,
                    entries=[],
                    leader_commit=0
                ))
                print(f"Server {server_id} is reachable")
                return True
            except Exception as e:
                print(f"Server {server_id} is unreachable: {str(e)}")
                return False
        
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(check_server_reachable, server_id)
                for server_id in servers.keys()
            ]
            reachable_servers = sum(1 for future in futures if future.result())
        
        print(f"Quorum check: have {reachable_servers} servers, need {majority_needed}")
        return reachable_servers >= majority_needed

    def _start_election(self):
        """Modified election start to check for quorum"""
        if self.current_state == NodeState.LEADER:
            return
        
        # First check if we have enough servers for quorum
        if not self._has_quorum_available():
            print(f"\nNot enough servers available for quorum. Remaining as follower.")
            self.current_state = NodeState.FOLLOWER
            self.leader_id = None  # No leader when no quorum
            return
                
        print(f"\n{'='*50}")
        print(f"{self.node_id} STARTING ELECTION")
        print(f"{'='*50}")
        
        self.current_state = NodeState.CANDIDATE
        current_term = self.state_manager.get_current_term() + 1
        self.state_manager.set_current_term(current_term)
        self.state_manager.set_voted_for(self.node_id)
        self.votes_received = 1
        
        # Reset election timeout when starting election
        self.election_timeout = randint(Config.MIN_ELECTION_TIMEOUT, Config.MAX_ELECTION_TIMEOUT) / 1000
        self.last_heartbeat = time.time()
        
        print(f"Became candidate for term {current_term}")
        
        self._request_votes()
        
        if self.current_state == NodeState.CANDIDATE:
            print(f"{self.node_id} failed to get majority votes, returning to follower state")
            self.current_state = NodeState.FOLLOWER
            self.leader_id = None  # Clear leader_id if election fails

    def _request_votes(self):
        """Send RequestVote RPCs with proper source port binding"""
        print(f"\n{self.node_id} requesting votes for term {self.state_manager.get_current_term()}")
        
        servers = Config.load_servers()
        print(f"Known servers: {list(servers.keys())}")

        def request_votes_concurrently(server_id):
            if server_id == self.node_id:
                return
                
            try:
                print(f"\nTrying to contact {server_id}")
                
                # Use connection manager for proper source port binding
                stub = self._get_stub_for_server(server_id)
                
                last_log_entry = self.state_manager.get_last_log_entry()
                request = raft_pb2.VoteRequest(
                    term=self.state_manager.get_current_term(),
                    candidate_id=self.node_id,
                    last_log_index=last_log_entry['index'] if last_log_entry else 0,
                    last_log_term=last_log_entry['term'] if last_log_entry else 0
                )
                
                print(f"Sending vote request to {server_id}: {request}")
                response = stub.RequestVote(request)
                print(f"Received response from {server_id}: {response}")
                
                if response.vote_granted:
                    self.votes_received += 1
                    print(f"Got vote from {server_id}! Total votes: {self.votes_received}")
                    self.election_flag = True
                    self._check_election_result()
                else:
                    print(f"{server_id} rejected our vote request")
                    
            except Exception as e:
                print(f"Failed to request vote from {server_id}: {str(e)}")

        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(request_votes_concurrently, server_id) 
                for server_id in servers if server_id != self.node_id
            ]
            
        if not self.election_flag:
            self._check_election_result()

    def _check_election_result(self):
        """Modified election check to ensure strict majority"""
        # Use original cluster size for majority calculation
        cluster_size = Config.load_cluster_size()
        if cluster_size == 0:
            cluster_size = len(Config.load_servers())
        
        majority_needed = (cluster_size // 2) + 1
        print(f"Votes received: {self.votes_received}, Need {majority_needed} for majority")
        
        if self.votes_received >= majority_needed:
            self._become_leader()
        else:
            print(f"Failed to achieve majority ({self.votes_received}/{cluster_size} votes)")
            self.current_state = NodeState.FOLLOWER
            self.leader_id = None

    def _become_leader(self):
        """Transition to leader state"""
        if self.current_state == NodeState.CANDIDATE:
            print(f"\n{'!'*50}")
            print(f"{self.node_id} BECOMING LEADER")
            print(f"{'!'*50}")
            
            self.current_state = NodeState.LEADER
            self.leader_id = self.node_id
            self.state_manager.set_voted_for(self.node_id)  # Explicitly set voted_for to self
            
            # Initialize leader state
            servers = Config.load_servers()
            last_log_index = self.state_manager.get_log_length() - 1
            
            for server_id in servers:
                if server_id != self.node_id:
                    self.state_manager.set_sent_length(last_log_index + 1, server_id)
                    self.state_manager.set_acked_length(0, server_id)
            
            # Start immediate sync with followers
            def initial_sync(server_id):
                if server_id != self.node_id:
                    try:
                        print(f"Initiating sync with follower {server_id}")
                        self.replicate_log(server_id)
                    except Exception as e:
                        print(f"Failed initial sync with {server_id}: {str(e)}")
            
            # Perform initial sync concurrently
            with ThreadPoolExecutor() as executor:
                executor.map(initial_sync, [s for s in servers if s != self.node_id])
            
            # Start heartbeat thread
            if not self.heartbeat_thread.is_alive():
                self.heartbeat_thread.start()

    def _sync_with_cluster(self):
        """Sync with the cluster when starting up"""
        print(f"\n{self.node_id} syncing with cluster...")
        servers = Config.load_servers()
        
        max_term = self.state_manager.get_current_term()
        max_log_length = self.state_manager.get_log_length()
        sync_target = None
        
        # Find the most up-to-date node
        for server_id in servers:
            if server_id != self.node_id:
                try:
                    stub = self._get_stub_for_server(server_id)
                    response = stub.AppendEntries(raft_pb2.AppendEntriesRequest(
                        term=self.state_manager.get_current_term(),
                        leader_id=self.node_id,
                        prev_log_index=-1,
                        prev_log_term=0,
                        entries=[],
                        leader_commit=0
                    ))
                    
                    if response.term > max_term or (response.term == max_term and response.ack > max_log_length):
                        max_term = response.term
                        max_log_length = response.ack
                        sync_target = server_id
                        
                except Exception as e:
                    print(f"Could not contact {server_id}: {str(e)}")
        
        # Sync with the most up-to-date node
        if sync_target:
            try:
                stub = self._get_stub_for_server(sync_target)
                response = stub.AppendEntries(raft_pb2.AppendEntriesRequest(
                    term=max_term,
                    leader_id=sync_target,
                    prev_log_index=-1,
                    prev_log_term=0,
                    entries=[],
                    leader_commit=0
                ))
                
                if response.success:
                    if max_term > self.state_manager.get_current_term():
                        self.state_manager.set_current_term(max_term)
                        self.state_manager.set_voted_for(sync_target)
                    self.current_state = NodeState.FOLLOWER
                    self.leader_id = sync_target
                    self.last_heartbeat = time.time()
                    print(f"Successfully synced with {sync_target}")
                    
            except Exception as e:
                print(f"Failed to sync with {sync_target}: {str(e)}")

    def RequestVote(self, request, context):
        """Handle incoming vote requests"""
        print(f"\n{'*'*50}")
        print(f"{self.node_id} RECEIVED VOTE REQUEST")
        print(f"{'*'*50}")
        print(f"From: {request.candidate_id}")
        print(f"Their term: {request.term}")
        print(f"Their last log index: {request.last_log_index}")
        print(f"Their last log term: {request.last_log_term}")
        print(f"Our term: {self.state_manager.get_current_term()}")
        print(f"Our commit index: {self.state_manager.get_commit_index()}")
        
        current_term = self.state_manager.get_current_term()
        
        # If we see a higher term, immediately become follower
        if request.term > current_term:
            print(f"Their term is higher - updating our term and becoming follower")
            self.current_state = NodeState.FOLLOWER
            self.state_manager.set_current_term(request.term)
            self.state_manager.set_voted_for(None)
            current_term = request.term
        
        # If they're behind, reject
        if request.term < current_term:
            print(f"Rejecting vote - their term ({request.term}) is lower than ours ({current_term})")
            return raft_pb2.VoteResponse(term=current_term, vote_granted=False)
        
        voted_for = self.state_manager.get_voted_for()
        
        # Critical fix: Compare with our last COMMITTED entry
        our_commit_index = self.state_manager.get_commit_index()
        our_last_committed_entry = None
        if our_commit_index >= 0:
            our_last_committed_entry = self.state_manager.get_log_entry(our_commit_index)
        
        our_last_committed_term = our_last_committed_entry['term'] if our_last_committed_entry else 0
        our_last_committed_index = our_last_committed_entry['index'] if our_last_committed_entry else -1
        
        log_is_current = False
        if request.last_log_term > our_last_committed_term:
            log_is_current = True
        elif request.last_log_term == our_last_committed_term:
            log_is_current = request.last_log_index >= our_last_committed_index
        
        print(f"Log currency check:")
        print(f"Our last committed term: {our_last_committed_term}, Their last term: {request.last_log_term}")
        print(f"Our last committed index: {our_last_committed_index}, Their last index: {request.last_log_index}")
        print(f"Log is current: {log_is_current}")
        
        # Only vote if we haven't voted and their log contains all our committed entries
        if (voted_for is None or voted_for == request.candidate_id) and log_is_current:
            print(f"Granting vote to {request.candidate_id}")
            self.current_state = NodeState.FOLLOWER
            self.state_manager.set_voted_for(request.candidate_id)
            self.leader_id = request.candidate_id
            self.last_heartbeat = time.time()
            return raft_pb2.VoteResponse(term=current_term, vote_granted=True)
        
        if not log_is_current:
            print(f"Rejecting vote - candidate's log is not up-to-date with our committed entries")
        else:
            print(f"Rejecting vote - already voted for {voted_for}")
        return raft_pb2.VoteResponse(term=current_term, vote_granted=False)

    def AppendEntries(self, request, context):
        """Handle incoming AppendEntries RPCs"""
        print(f"\nReceived AppendEntries from {request.leader_id}")
        print(f"Leader commit: {request.leader_commit}")
        print(f"Our commit: {self.state_manager.get_commit_index()}")
        print(f"Our log length: {self.state_manager.get_log_length()}")

        print(f"Context is ****************: {context.peer()}")
        
        current_term = self.state_manager.get_current_term()
        
        # Always reset heartbeat on AppendEntries from valid leader
        if request.term >= current_term:
            self.last_heartbeat = time.time()
        
        # If term is greater, become follower
        if request.term > current_term:
            print(f"Their term is higher - becoming follower")
            self.current_state = NodeState.FOLLOWER
            self.state_manager.set_current_term(request.term)
            self.state_manager.set_voted_for(request.leader_id)  # Set voted_for to leader
            current_term = request.term
            self.leader_id = request.leader_id
        
        # If term is equal and we haven't voted for anyone, accept this leader
        if request.term == current_term:
            if self.state_manager.get_voted_for() is None:
                print(f"Setting leader as {request.leader_id}")
                self.state_manager.set_voted_for(request.leader_id)
            if self.current_state != NodeState.FOLLOWER:
                print(f"Becoming follower - same term leader")
                self.current_state = NodeState.FOLLOWER
            self.leader_id = request.leader_id
        
        # Reject if term is lower
        if request.term < current_term:
            print(f"Rejecting AppendEntries - their term ({request.term}) is lower")
            return raft_pb2.AppendEntriesResponse(
                term=current_term,
                success=False,
                ack=self.state_manager.get_log_length()
            )
        
        # Check log consistency
        prev_log_matches = True
        if request.prev_log_index >= 0:
            prev_entry = self.state_manager.get_log_entry(request.prev_log_index)
            if not prev_entry or prev_entry['term'] != request.prev_log_term:
                prev_log_matches = False
        
        if not prev_log_matches:
            print(f"Log mismatch at index {request.prev_log_index}")
            return raft_pb2.AppendEntriesResponse(
                term=current_term,
                success=False,
                ack=self.state_manager.get_log_length()
            )
        
        # Process entries
        if request.entries:
            print(f"Processing {len(request.entries)} entries")
            # Remove conflicting entries
            self.state_manager.delete_entries_from(request.prev_log_index + 1)
            
            # Append new entries
            for entry in request.entries:
                self.state_manager.append_log_entry(
                    term=entry.term,
                    command=entry.command,
                    key=entry.key,
                    value=entry.value,
                    client_id=entry.client_id,
                    request_id=entry.request_id
                )
        
        # Update commit index
        if request.leader_commit >= self.state_manager.get_commit_index():
            commit_index = min(request.leader_commit, self.state_manager.get_log_length() - 1)
            if commit_index >= 0:
                print(f"Updating commit index to {commit_index}")
                self.state_manager.set_commit_index(commit_index)
                self.state_manager.commit_logs_up_to(commit_index)
        
        return raft_pb2.AppendEntriesResponse(
            term=current_term,
            success=True,
            ack=self.state_manager.get_log_length()
        )
    
    def log_acknowledgement(self, request):
        """Process log acknowledgment from followers"""
        if request.term == self.state_manager.get_current_term() and self.current_state == NodeState.LEADER:
            follower = request.follower_id
            
            if request.success and request.ack >= self.state_manager.get_acked_length(follower):
                # Update acked length for this follower
                self.state_manager.set_acked_length(request.ack, follower)
                self._commit_log_entries()
            elif self.state_manager.get_sent_length(follower) > 0:
                # Decrement and retry
                self.state_manager.set_sent_length(
                    self.state_manager.get_sent_length(follower) - 1,
                    follower
                )
                self.replicate_log(follower)
        elif request.term > self.state_manager.get_current_term():
            # Step down if we see higher term
            self.current_state = NodeState.FOLLOWER
            self.state_manager.set_current_term(request.term)
            self.state_manager.set_voted_for(None)

    def _commit_log_entries(self):
        """More aggressive commit strategy with better linearizability"""
        if self.current_state != NodeState.LEADER:
            print("Not leader, skipping commit check")
            return False
        
        # First check if we have quorum available
        # if not self._has_quorum_available():
        #     print("No quorum available, cannot commit entries")
        #     return False
            
        servers = Config.load_servers()
        cluster_size = Config.load_cluster_size() or len(servers)
        majority_needed = (cluster_size // 2) + 1
        
        start_index = self.state_manager.get_commit_index() + 1
        log_length = self.state_manager.get_log_length()
        
        print(f"\nChecking commits from index {start_index} to {log_length-1}")
        print(f"Need {majority_needed} out of {cluster_size} for majority")
        
        # Optimize by checking all entries quickly 
        for current_idx in range(start_index, log_length):
            acks = 1  # Count leader
            acking_servers = [self.node_id]
            
            for server_id in servers:
                if server_id != self.node_id:
                    acked_length = self.state_manager.get_acked_length(server_id)
                    if acked_length >= current_idx + 1:
                        acks += 1
                        acking_servers.append(server_id)
                        
                        if acks >= majority_needed:
                            # Got majority - commit immediately
                            print(f"Got majority for index {current_idx} from {acking_servers}")
                            self.state_manager.set_commit_index(current_idx)
                            # Critical: Commit before moving on
                            self.state_manager.commit_logs_up_to(current_idx)
                            continue
                            
            if acks < majority_needed:
                print(f"Only got {acks}/{majority_needed} acks for index {current_idx}")
                return False
                
        return True

    def replicate_log(self, follower_id: str):
        """Optimized replication with proper source port binding"""
        next_index = self.state_manager.get_sent_length(follower_id)
        print(f"\nAttempting replication to {follower_id}")
        print(f"Next index: {next_index}")
        print(f"Our log length: {self.state_manager.get_log_length()}")
        print(f"Our log: {self.state_manager.state['log']}")
        print(f"Our commit index: {self.state_manager.get_commit_index()}")

        try:
            # Use connection manager for proper source port binding
            stub = self._get_stub_for_server(follower_id)
            
            # Batch process entries - send up to 10 entries at once
            entries = []
            batch_end = min(next_index + 10, self.state_manager.get_log_length())
            
            for i in range(next_index, batch_end):
                entry = self.state_manager.get_log_entry(i)
                if entry:
                    entries.append(raft_pb2.LogEntry(
                        term=entry['term'],
                        index=entry['index'],
                        command=entry['command'],
                        key=entry['key'],
                        value=entry['value'],
                        client_id=entry.get('client_id'),
                        request_id=entry.get('request_id')
                    ))

            print(f"Sending batch of {len(entries)} entries to {follower_id}")
            
            # Get previous log info
            prev_index = next_index - 1
            prev_term = 0
            if prev_index >= 0:
                prev_entry = self.state_manager.get_log_entry(prev_index)
                if prev_entry:
                    prev_term = prev_entry['term']

            print(f"Previous index: {prev_index}, Previous term: {prev_term}")
            
            request = raft_pb2.AppendEntriesRequest(
                term=self.state_manager.get_current_term(),
                leader_id=self.node_id,
                prev_log_index=prev_index,
                prev_log_term=prev_term,
                entries=entries,
                leader_commit=self.state_manager.get_commit_index()
            )

            print(f"Sending AppendEntries to {follower_id}:")
            print(f"Term: {request.term}")
            print(f"Leader commit: {request.leader_commit}")
            print(f"Number of entries: {len(entries)}")
            
            response = stub.AppendEntries(request, timeout=0.5)
            print(f"Got response from {follower_id}:")
            print(f"Success: {response.success}")
            print(f"Term: {response.term}")
            print(f"Ack: {response.ack}")
            
            if response.success:
                new_sent_length = next_index + len(entries)
                self.state_manager.set_sent_length(new_sent_length, follower_id)
                self.state_manager.set_acked_length(response.ack, follower_id)
                print(f"Updated sent_length to {new_sent_length} and acked_length to {response.ack}")
                
                self._commit_log_entries()
                return response
            
            print(f"Replication failed, will retry from index {next_index-1}")
            return None
                        
        except Exception as e:
            print(f"Failed to replicate to {follower_id}: {str(e)}")
            return None

    def _send_heartbeats(self):
        """Updated heartbeat method using connection manager"""
        while True:
            if self.current_state != NodeState.LEADER:
                time.sleep(0.1)
                continue
                
            print(f"\n{self.node_id} sending heartbeats/sync as leader")
            servers = Config.load_servers()
            
            def sync_server(server_id):
                if server_id == self.node_id:
                    return True
                    
                try:
                    # Get stub with proper source port binding
                    stub = self._get_stub_for_server(server_id)
                    
                    # Check if server needs synchronization
                    server_acked = self.state_manager.get_acked_length(server_id)
                    log_length = self.state_manager.get_log_length()
                    needs_sync = server_acked < log_length
                    
                    if needs_sync:
                        print(f"Server {server_id} needs sync: acked={server_acked}, log_length={log_length}")
                    
                    # Prepare entries for sync or empty for heartbeat
                    entries = []
                    prev_log_index = -1
                    prev_log_term = 0
                    
                    if needs_sync:
                        start_index = 0 if server_acked == 0 else server_acked
                        for i in range(start_index, log_length):
                            entry = self.state_manager.get_log_entry(i)
                            if entry:
                                entries.append(raft_pb2.LogEntry(
                                    term=entry['term'],
                                    index=entry['index'],
                                    command=entry['command'],
                                    key=entry['key'],
                                    value=entry['value']
                                ))
                        
                        if start_index > 0:
                            prev_entry = self.state_manager.get_log_entry(start_index - 1)
                            if prev_entry:
                                prev_log_index = prev_entry['index']
                                prev_log_term = prev_entry['term']
                    
                    # Send AppendEntries
                    request = raft_pb2.AppendEntriesRequest(
                        term=self.state_manager.get_current_term(),
                        leader_id=self.node_id,
                        prev_log_index=prev_log_index,
                        prev_log_term=prev_log_term,
                        entries=entries,
                        leader_commit=self.state_manager.get_commit_index()
                    )
                    
                    if needs_sync:
                        print(f"Sending {len(entries)} entries to {server_id} for sync")
                    
                    response = stub.AppendEntries(request, timeout=0.5)
                    
                    if response.term > self.state_manager.get_current_term():
                        print(f"Stepping down: found higher term from {server_id}")
                        self.current_state = NodeState.FOLLOWER
                        self.state_manager.set_current_term(response.term)
                        self.state_manager.set_voted_for(None)
                    elif response.success and needs_sync:
                        self.state_manager.set_sent_length(log_length, server_id)
                        self.state_manager.set_acked_length(response.ack, server_id)
                        print(f"Sync successful for {server_id}: acked={response.ack}")
                    return True
                        
                except Exception as e:
                    print(f"Failed sync/heartbeat to {server_id}: {str(e)}")
                    return False
            
            # Send heartbeats/sync concurrently
            reachable_servers = 1  # Count self
            with ThreadPoolExecutor() as executor:
                futures = [
                    executor.submit(sync_server, server_id)
                    for server_id in servers if server_id != self.node_id
                ]
                reachable_servers += sum(1 for future in futures if future.result())
            
            # Check quorum and commit entries if possible
            cluster_size = Config.load_cluster_size() or len(servers)
            majority_needed = (cluster_size // 2) + 1
            
            if reachable_servers >= majority_needed:
                self._commit_log_entries()
            
            time.sleep(Config.HEARTBEAT_INTERVAL / 1000)

    def replicate_log_to_all(self):
        """Replicate log to all followers with proper source port binding"""
        servers = Config.load_servers()
        cluster_size = len(servers)
        successful_replications = 1  # Count self
        
        def replicate_to_server(server_id):
            if server_id == self.node_id:
                return True
                
            try:
                response = self.replicate_log(server_id)
                return response is not None and response.success
            except Exception as e:
                print(f"Failed to replicate to {server_id}: {str(e)}")
                return False
        
        # Replicate to all servers concurrently
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(replicate_to_server, server_id)
                for server_id in servers
            ]
            
            # Wait for all replications to complete
            for future in futures:
                if future.result():
                    successful_replications += 1
        
        # Check if we achieved majority
        majority = (cluster_size // 2) + 1
        print(f"Successful replications: {successful_replications}/{cluster_size} (need {majority})")
        
        return successful_replications >= majority

    def SendCommand(self, request, context):
        """Updated command handling with more efficient consensus"""
        if self.current_state != NodeState.LEADER:
            print(f"Forwarding command to leader {self.leader_id}")
            try:
                if self.leader_id:
                    # Get stub with proper source port binding
                    stub = self._get_stub_for_server(self.leader_id)
                    return stub.SendCommand(request)
                else:
                    # No leader - trigger election if we're a follower
                    if self.current_state == NodeState.FOLLOWER:
                        print("No leader available - initiating election")
                        self.last_heartbeat = time.time() - (2 * self.election_timeout)
                        # Give brief time for election
                        time.sleep(0.1)

                return raft_pb2.SendCommandResponse(
                    success=False,
                    message="No leader available"
                )
            except Exception as e:
                print(f"Failed to forward to leader: {str(e)}")
                return raft_pb2.SendCommandResponse(
                    success=False,
                    message=f"Failed to forward to leader: {str(e)}"
                )

        print(f"\n{self.node_id} received command as leader: {request.msg}")
        parts = request.msg.split()
        client_id = getattr(request, 'client_id', None)
        request_id = getattr(request, 'request_id', None)

        # Check duplicates first
        if client_id is not None and request_id is not None:
            is_duplicate, prev_result = self.state_manager.is_duplicate_request(client_id, request_id)
            if is_duplicate:
                print(f"Duplicate request detected, returning cached result: {prev_result}")
                return raft_pb2.SendCommandResponse(
                    success=True,
                    message=prev_result
                )

        # Perform the quorum check only once
        if not self._has_quorum_available():
            print("No quorum available - cannot process command")
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "No quorum available")
            return raft_pb2.SendCommandResponse(
                success=False,
                message="No quorum available"  # Changed message to be more specific
            )

        if len(parts) == 2:  # GET operation
            cmd, key = parts
            # For reads, we could optionally implement a read-only quorum check
            # instead of full log replication
            value = self.state_manager.get_data(key)
            if not value:
                print(f"Key {key} not found")
                return raft_pb2.SendCommandResponse(
                    success=False,
                    message=f"Key {key} not found"
                )

            # Only append to log and replicate if we have a value
            entry_index = self.state_manager.append_log_entry(
                term=self.state_manager.get_current_term(),
                command=cmd,
                key=key,
                value="",
                client_id=client_id,
                request_id=request_id
            )

            if self.replicate_and_commit(entry_index):
                if client_id is not None and request_id is not None:
                    self.state_manager.record_request(client_id, request_id, value)
                return raft_pb2.SendCommandResponse(
                    success=True,
                    message=value
                )
            # If replication failed, still return the value but mark as potentially stale
            return raft_pb2.SendCommandResponse(
                success=True,
                message=value
            )

        elif len(parts) == 3:  # PUT or REPLACE operation
            cmd, key, value = parts
            
            # For REPLACE, check existence before attempting replication
            if cmd == "REPLACE" and not self.state_manager.get_data(key):
                return raft_pb2.SendCommandResponse(
                    success=False,
                    message=f"Key {key} not found for REPLACE operation"
                )

            # For writes, we must achieve consensus
            entry_index = self.state_manager.append_log_entry(
                term=self.state_manager.get_current_term(),
                command=cmd,
                key=key,
                value=value,
                client_id=client_id,
                request_id=request_id
            )

            if self.replicate_and_commit(entry_index):
                if client_id is not None and request_id is not None:
                    self.state_manager.record_request(client_id, request_id, value)
                return raft_pb2.SendCommandResponse(
                    success=True,
                    message=value
                )

            # Failed to achieve consensus
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Failed to achieve consensus")
            return raft_pb2.SendCommandResponse(
                success=False,
                message="Unable to complete write operation"  # More specific error
            )

        return raft_pb2.SendCommandResponse(
            success=False,
            message="Invalid command format"
        )
            
    def replicate_and_commit(self, entry_index):
        """Helper to handle replication and committing"""
        if self.current_state != NodeState.LEADER:
            return False
            
        print(f"Attempting to replicate and commit entry {entry_index}")
        servers = Config.load_servers()
        cluster_size = Config.load_cluster_size() or len(servers)
        majority_needed = (cluster_size // 2) + 1
        successful_acks = 1  # Count leader's own log

        def check_server_ack(server_id):
            if server_id == self.node_id:
                return True
            try:
                # First check if server has already acked this entry
                acked_length = self.state_manager.get_acked_length(server_id)
                if acked_length > entry_index:
                    return True
                    
                # If not acked, try replication
                response = self.replicate_log(server_id)
                return response is not None and response.success
                    
            except Exception as e:
                print(f"Failed to check/replicate to {server_id}: {str(e)}")
                return False

        remaining_servers = [sid for sid in servers.keys() if sid != self.node_id]
        with ThreadPoolExecutor(max_workers=len(remaining_servers)) as executor:
            futures = [
                executor.submit(check_server_ack, server_id) 
                for server_id in remaining_servers
            ]
            
            for future in futures:
                try:
                    if future.result(timeout=0.1):
                        successful_acks += 1
                        if successful_acks >= majority_needed:
                            break
                except Exception:
                    continue

        print(f"Got {successful_acks} acks out of {cluster_size} (need {majority_needed})")
        if successful_acks >= majority_needed:
            if entry_index > self.state_manager.get_commit_index():
                self.state_manager.set_commit_index(entry_index)
                self.state_manager.commit_logs_up_to(entry_index)
            return True
                
        return False
    
    def cleanup(self):
        """Cleanup method to properly close connections"""
        try:
            # Stop the server
            if self.server:
                self.server.stop(0)
                print("gRPC server stopped")
            
            # Close all connections
            if hasattr(self, 'connection_manager'):
                self.connection_manager.close_all()
                print("All connections closed")
                
            # Remove from configuration
            servers = Config.load_servers()
            if self.node_id in servers:
                del servers[self.node_id]
                Config.save_servers(servers)
                print(f"Removed {self.node_id} from cluster configuration")
                
        except Exception as e:
            print(f"Error during cleanup: {str(e)}")