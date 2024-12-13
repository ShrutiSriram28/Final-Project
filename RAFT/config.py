from enum import Enum
import json

class NodeState(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"

class Config:
    # Server settings
    BASE_PORT = 9001 #CHANGE MADE
    HOST = 'localhost'

    REQUEST_TRACKER_FILE = 'request_tracker.json'
    
    # Timing settings (in milliseconds)
    MIN_ELECTION_TIMEOUT = 150
    MAX_ELECTION_TIMEOUT = 300
    HEARTBEAT_INTERVAL = 50
    
    # File for storing server information
    SERVERS_FILE = 'servers.json'
    CLUSTER_SIZE_FILE = 'cluster_size.json'
    CLUSTER_MEMBERS_FILE = 'cluster_members.json'

    @staticmethod
    def save_servers(servers):
        with open(Config.SERVERS_FILE, 'w') as f:
            json.dump(servers, f)

    @staticmethod
    def load_servers():
        try:
            with open(Config.SERVERS_FILE, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return {}
    
    @staticmethod
    def save_cluster_size(size):
        """Save the original cluster size"""
        with open(Config.CLUSTER_SIZE_FILE, 'w') as f:
            json.dump({"size": size}, f)
    
    @staticmethod
    def load_cluster_size():
        """Load the original cluster size"""
        try:
            with open(Config.CLUSTER_SIZE_FILE, 'r') as f:
                data = json.load(f)
                return data.get("size", 0)
        except FileNotFoundError:
            return 0
            
    @staticmethod
    def save_cluster_members(members):
        """Save the set of servers that are part of the cluster"""
        with open(Config.CLUSTER_MEMBERS_FILE, 'w') as f:
            json.dump({"members": list(members)}, f)
    
    @staticmethod
    def load_cluster_members():
        """Load the set of servers that are part of the cluster"""
        try:
            with open(Config.CLUSTER_MEMBERS_FILE, 'r') as f:
                data = json.load(f)
                return set(data.get("members", []))
        except FileNotFoundError:
            return set()