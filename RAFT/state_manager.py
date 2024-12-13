import json
import os
from typing import List, Dict, Optional
from config import Config  # Import Config for quorum calculation

class StateManager:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.state_file = f'state_{node_id}.json'
        self.state = self._load_state()

    def _load_state(self) -> Dict:
        if os.path.exists(self.state_file):
            with open(self.state_file, 'r') as f:
                state = json.load(f)
                state['sent_length'] = {}
                state['acked_length'] = {}
                if 'request_history' not in state:  # Add request tracking to state
                    state['request_history'] = {}
                return state
        
        initial_state = {
            'current_term': 0,
            'voted_for': None,
            'log': [],
            'commit_index': 0,
            'last_applied': 0,
            'sent_length': {},  # Track last sent index for each follower
            'acked_length': {}, # Track last acknowledged index for each follower
            'data': {},         # Key-value store for the state machine
            'request_history': {}
        }
        
        # Save initial state
        with open(self.state_file, 'w') as f:
            json.dump(initial_state, f)
            
        return initial_state

    def save_state(self):
        with open(self.state_file, 'w') as f:
            json.dump(self.state, f)

    def is_duplicate_request(self, client_id: int, request_id: int) -> tuple[bool, str]:
        """Check if request is duplicate and return result if it exists"""
        key = f"{client_id}_{request_id}"
        if key in self.state['request_history']:
            return True, self.state['request_history'][key]
        return False, ""
    
    def record_request(self, client_id: int, request_id: int, result: str):
        """Record the result of a processed request"""
        if client_id is not None and request_id is not None:
            key = f"{client_id}_{request_id}"
            self.state['request_history'][key] = result
            self.save_state()

    # Term management
    def get_current_term(self) -> int:
        return self.state['current_term']

    def set_current_term(self, term: int):
        """Set current term with logging"""
        old_term = self.state['current_term']
        self.state['current_term'] = term
        print(f"Term change: {old_term} -> {term}")
        self.save_state()

    # Vote management
    def get_voted_for(self) -> Optional[str]:
        return self.state['voted_for']

    def set_voted_for(self, candidate_id: Optional[str]):
        """Set voted_for with logging"""
        old_vote = self.state['voted_for']
        self.state['voted_for'] = candidate_id
        print(f"Vote change: {old_vote} -> {candidate_id}")
        self.save_state()

    # Commit index management
    def get_commit_index(self) -> int:
        return self.state['commit_index']

    def set_commit_index(self, value: int):
        self.state['commit_index'] = value
        self.save_state()

    # Last applied management
    def get_last_applied(self) -> int:
        return self.state['last_applied']

    def set_last_applied(self, value: int):
        self.state['last_applied'] = value
        self.save_state()

    # Replication tracking
    def get_sent_length(self, node_id: Optional[str]) -> int:
        return self.state['sent_length'].get(node_id, 0)

    def set_sent_length(self, value: int, node_id: Optional[str]):
        self.state['sent_length'][node_id] = value
        self.save_state()

    def get_acked_length(self, node_id: Optional[str]) -> int:
        return self.state['acked_length'].get(node_id, 0)

    def set_acked_length(self, value: int, node_id: Optional[str]):
        self.state['acked_length'][node_id] = value
        self.save_state()

    # State machine operations
    def get_data(self, key: str) -> str:
        """Get value from key-value store"""
        return self.state['data'].get(key, "")
        
    def set_data(self, key: str, value: str):
        """Put operation for key-value store"""
        print(f"Setting data: key={key}, value={value}")  # Debug log
        self.state['data'][key] = value
        self.save_state()

    def replace_data(self, key: str, value: str) -> bool:
        """Replace operation for key-value store with success indicator"""
        if key in self.state['data']:
            self.state['data'][key] = value
            self.save_state()
            return True
        return False
        
    def delete_data(self, key: str):
        """Delete key from key-value store"""
        if key in self.state['data']:
            del self.state['data'][key]
            self.save_state()

    def get_all_data(self) -> Dict[str, str]:
        """Get entire key-value store"""
        return self.state['data']

    # Log operations
    def append_log_entry(self, term: int, command: str, key: str, value: str, client_id: Optional[int] = None, request_id: Optional[int] = None) -> int:
        """Append a new entry to the log"""
        index = len(self.state['log'])
        entry = {
            'term': term,
            'index': index,
            'command': command,
            'key': key,
            'value': value,
            'client_id': client_id,  # Add client tracking
            'request_id': request_id
        }
        
        # Check for conflicting entries
        if index < len(self.state['log']):
            if self.state['log'][index]['term'] != term:
                # Delete conflicting entries
                self.delete_entries_from(index)
        
        self.state['log'].append(entry)
        self.save_state()
        return index

    def get_last_log_entry(self) -> Optional[Dict]:
        return self.state['log'][-1] if self.state['log'] else None

    def get_log_entry(self, index: int) -> Optional[Dict]:
        return self.state['log'][index] if 0 <= index < len(self.state['log']) else None
    
    def get_command(self) -> Optional[str]:
        return self.state['log'][-1]["command"] if len(self.state['log']) else None

    def get_log_length(self) -> int:
        return len(self.state['log'])

    def delete_entries_from(self, start_index: int):
        """Delete conflicting entries starting from start_index"""
        self.state['log'] = self.state['log'][:start_index]
        self.save_state()

    def commit_logs_up_to(self, index: int):
        """Apply committed logs to the state machine"""
        print(f"\nCommitting logs up to index {index}")
        print(f"Current last_applied: {self.state['last_applied']}")
        print(f"Current data store: {self.state['data']}")
        
        result_value = ""
        
        # Only process entries we haven't applied yet
        for i in range(self.state['last_applied'], index + 1):
            entry = self.get_log_entry(i)
            if not entry:
                print(f"No entry found at index {i}, stopping commit")
                break
                
            print(f"Applying entry {i}: {entry}")
            key = entry['key']
            value = entry['value']
            
            if entry['command'] == "PUT":
                print(f"Executing PUT: {key} = {value}")
                self.set_data(key, value)
                result_value = value
            elif entry['command'] == "GET":
                result_value = self.get_data(key)
                print(f"Executing GET: {key} = {result_value}")
            elif entry['command'] == "REPLACE":
                if key in self.state['data']:
                    print(f"Executing REPLACE: {key} = {value}")
                    self.set_data(key, value)
                    result_value = value
                else:
                    print(f"Skipping REPLACE: key {key} not found")
                    result_value = ""
        
            # Record request result if client info exists
            if entry.get('client_id') is not None and entry.get('request_id') is not None:
                self.record_request(entry['client_id'], entry['request_id'], result_value)
                print(f"Recorded request for client {entry['client_id']}: {result_value}")  # Debug log
        
        # Update last_applied
        self.state['last_applied'] = index
        print(f"Updated last_applied to {index}")
        print(f"Final data store state: {self.state['data']}")  # Debug log
        self.save_state()
        
        return result_value

    def get_quorum_size(self) -> int:
        """Calculate size needed for quorum based on original cluster size"""
        cluster_size = Config.load_cluster_size()
        if cluster_size == 0:
            # Fallback to current servers if no cluster size is saved
            cluster_size = len(Config.load_servers())
        return (cluster_size + 1) // 2

    def is_log_committed(self, index: int, acked_lengths: Dict[str, int]) -> bool:
        """Check if a log entry has been replicated to a majority"""
        acks = 1  # Count self
        for node_id, acked_length in acked_lengths.items():
            if acked_length > index:
                acks += 1
        return acks > self.get_quorum_size()