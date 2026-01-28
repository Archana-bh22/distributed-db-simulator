from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any, Union, Set
from enum import Enum
import hashlib
import json

NODE_CONFIG = {
    "n1": {"host": "127.0.0.1", "port": 5001, "cluster": "C1"},
    "n2": {"host": "127.0.0.1", "port": 5002, "cluster": "C1"},
    "n3": {"host": "127.0.0.1", "port": 5003, "cluster": "C1"},
    "n4": {"host": "127.0.0.1", "port": 5004, "cluster": "C2"},
    "n5": {"host": "127.0.0.1", "port": 5005, "cluster": "C2"},
    "n6": {"host": "127.0.0.1", "port": 5006, "cluster": "C2"},
    "n7": {"host": "127.0.0.1", "port": 5007, "cluster": "C3"},
    "n8": {"host": "127.0.0.1", "port": 5008, "cluster": "C3"},
    "n9": {"host": "127.0.0.1", "port": 5009, "cluster": "C3"},
}

CLUSTER_CONFIG = {
    "C1": {"nodes": ["n1", "n2", "n3"], "initial_leader": "n1"},
    "C2": {"nodes": ["n4", "n5", "n6"], "initial_leader": "n4"},
    "C3": {"nodes": ["n7", "n8", "n9"], "initial_leader": "n7"},
}

# Shard ranges: C1=[1-3000], C2=[3001-6000], C3=[6001-9000]
INITIAL_SHARD_RANGES = {
    "C1": (1, 3000),
    "C2": (3001, 6000),
    "C3": (6001, 9000),
}

TOTAL_DATA_ITEMS = 9000
INITIAL_BALANCE = 10

CONTROLLER_HOST = "127.0.0.1"
CONTROLLER_PORT = 6000

# Timeouts (milliseconds)
LEADER_TIMEOUT_MS = 3000
PREPARE_SUPPRESSION_MS = 800
HEARTBEAT_INTERVAL_MS = 400
CLIENT_TIMEOUT_MS = 2000
TPC_TIMEOUT_MS = 5000
TPC_RETRY_INTERVAL_MS = 1000

# Message types
MSG_REQUEST = "REQUEST"
MSG_REPLY = "REPLY"
MSG_PREPARE = "PREPARE"
MSG_ACK = "ACK"
MSG_ACCEPT = "ACCEPT"
MSG_ACCEPTED = "ACCEPTED"
MSG_COMMIT = "COMMIT"
MSG_NEW_VIEW = "NEW_VIEW"
MSG_HEARTBEAT = "HEARTBEAT"

MSG_2PC_PREPARE = "2PC_PREPARE"
MSG_2PC_PREPARED = "2PC_PREPARED"
MSG_2PC_ABORT = "2PC_ABORT"
MSG_2PC_COMMIT = "2PC_COMMIT"
MSG_2PC_ACK = "2PC_ACK"

MSG_BALANCE_REQUEST = "BALANCE_REQUEST"
MSG_BALANCE_REPLY = "BALANCE_REPLY"
MSG_CONTROL = "CONTROL"
MSG_LEADER_ANNOUNCE = "LEADER_ANNOUNCE"

class AcceptFlag(str, Enum):
    NORMAL = "N"
    PREPARE = "P"
    COMMIT = "C"
    ABORT = "A"

class TxnState(str, Enum):
    INIT = "INIT"
    PREPARING = "PREPARING"
    PREPARED = "PREPARED"
    COMMITTING = "COMMITTING"
    COMMITTED = "COMMITTED"
    ABORTING = "ABORTING"
    ABORTED = "ABORTED"

class TxnType(str, Enum):
    INTRA_SHARD = "INTRA"
    CROSS_SHARD = "CROSS"
    READ_ONLY = "READ"

Ballot = Tuple[int, str]

@dataclass
class Transaction:
    sender: int
    receiver: int
    amount: int
    txn_type: TxnType = TxnType.INTRA_SHARD
    
    def __hash__(self):
        return hash((self.sender, self.receiver, self.amount))
    
    def to_dict(self) -> Dict:
        return {
            "sender": self.sender,
            "receiver": self.receiver,
            "amount": self.amount,
            "txn_type": self.txn_type.value
        }
    
    @staticmethod
    def from_dict(d: Dict) -> 'Transaction':
        return Transaction(
            sender=int(d["sender"]),
            receiver=int(d["receiver"]),
            amount=int(d["amount"]),
            txn_type=TxnType(d.get("txn_type", TxnType.INTRA_SHARD.value))
        )

@dataclass
class Request:
    client_id: str
    timestamp: int
    txn: Transaction
    request_id: str = ""
    
    def __post_init__(self):
        if not self.request_id:
            self.request_id = f"{self.client_id}:{self.timestamp}"
    
    def to_dict(self) -> Dict:
        return {
            "client_id": self.client_id,
            "timestamp": self.timestamp,
            "txn": self.txn.to_dict(),
            "request_id": self.request_id
        }
    
    @staticmethod
    def from_dict(d: Dict) -> 'Request':
        return Request(
            client_id=d["client_id"],
            timestamp=int(d["timestamp"]),
            txn=Transaction.from_dict(d["txn"]),
            request_id=d.get("request_id", "")
        )
    
    def digest(self) -> str:
        data = json.dumps(self.to_dict(), sort_keys=True)
        return hashlib.sha256(data.encode()).hexdigest()[:16]

@dataclass
class Reply:
    ballot: Ballot
    client_id: str
    timestamp: int
    result: str
    balance: Optional[int] = None
    
    def to_dict(self) -> Dict:
        return {
            "ballot": list(self.ballot) if self.ballot else None,
            "client_id": self.client_id,
            "timestamp": self.timestamp,
            "result": self.result,
            "balance": self.balance
        }

@dataclass
class LogEntry:
    seq: int
    ballot: Optional[Ballot]
    value: Optional[Union[Request, str]]
    accepted: bool = False
    committed: bool = False
    executed: bool = False
    accept_flag: AcceptFlag = AcceptFlag.NORMAL
    txn_id: Optional[str] = None
    
    def to_dict(self) -> Dict:
        val = None
        if isinstance(self.value, Request):
            val = self.value.to_dict()
        elif self.value:
            val = str(self.value)
        return {
            "seq": self.seq,
            "ballot": list(self.ballot) if self.ballot else None,
            "value": val,
            "accepted": self.accepted,
            "committed": self.committed,
            "executed": self.executed,
            "accept_flag": self.accept_flag.value,
            "txn_id": self.txn_id
        }

@dataclass
class WALEntry:
    txn_id: str
    account_id: int
    before_balance: int
    after_balance: int
    timestamp: float

@dataclass
class CrossShardTxn:
    txn_id: str
    request: Request
    state: TxnState = TxnState.INIT
    coordinator_cluster: str = ""
    participant_cluster: str = ""
    prepare_seq: Optional[int] = None
    final_seq: Optional[int] = None
    created_at: float = 0.0
    prepared_at: float = 0.0
    completed_at: float = 0.0
    is_coordinator: bool = False
    prepared_received: bool = False
    ack_received: bool = False

class ShardMapping:
    def __init__(self):
        self._mapping: Dict[int, str] = {}
        self._initialize_default_mapping()
    
    def _initialize_default_mapping(self):
        for cluster_id, (start, end) in INITIAL_SHARD_RANGES.items():
            for item_id in range(start, end + 1):
                self._mapping[item_id] = cluster_id
    
    def get_cluster(self, item_id: int) -> str:
        return self._mapping.get(item_id, "C1")
    
    def get_items_for_cluster(self, cluster_id: str) -> List[int]:
        return [k for k, v in self._mapping.items() if v == cluster_id]
    
    def is_intra_shard(self, sender: int, receiver: int) -> bool:
        return self.get_cluster(sender) == self.get_cluster(receiver)
    
    def update_mapping(self, item_id: int, new_cluster: str) -> Optional[str]:
        old_cluster = self._mapping.get(item_id)
        if old_cluster != new_cluster:
            self._mapping[item_id] = new_cluster
            return old_cluster
        return None
    
    def get_full_mapping(self) -> Dict[int, str]:
        return self._mapping.copy()
    
    def reset(self):
        self._mapping.clear()
        self._initialize_default_mapping()

SHARD_MAP = ShardMapping()

def get_cluster_for_node(node_id: str) -> str:
    return NODE_CONFIG[node_id]["cluster"]

def get_nodes_in_cluster(cluster_id: str) -> List[str]:
    return CLUSTER_CONFIG[cluster_id]["nodes"]

def get_initial_leader(cluster_id: str) -> str:
    return CLUSTER_CONFIG[cluster_id]["initial_leader"]

def get_other_clusters(cluster_id: str) -> List[str]:
    return [c for c in CLUSTER_CONFIG.keys() if c != cluster_id]

def get_cluster_for_item(item_id: int) -> str:
    return SHARD_MAP.get_cluster(item_id)

def is_cross_shard(txn: Transaction) -> bool:
    return not SHARD_MAP.is_intra_shard(txn.sender, txn.receiver)

def get_quorum_size(cluster_id: str) -> int:
    n = len(CLUSTER_CONFIG[cluster_id]["nodes"])
    return n // 2 + 1

def base_msg(msg_type: str, src: str, dst: str) -> Dict[str, Any]:
    return {"type": msg_type, "src": src, "dst": dst}

def make_reply(src: str, dst: str, reply: Reply) -> Dict:
    msg = base_msg(MSG_REPLY, src, dst)
    msg.update(reply.to_dict())
    return msg

def make_balance_reply(src: str, dst: str, client_id: str, timestamp: int, 
                       balance: int, result: str = "success") -> Dict:
    msg = base_msg(MSG_BALANCE_REPLY, src, dst)
    msg.update({
        "client_id": client_id,
        "timestamp": timestamp,
        "balance": balance,
        "result": result
    })
    return msg

def generate_txn_id(request: Request) -> str:
    return f"txn:{request.client_id}:{request.timestamp}:{request.txn.sender}:{request.txn.receiver}"

class ClusterLeaderTracker:
    def __init__(self):
        self._leaders: Dict[str, str] = {}
        for cluster_id, config in CLUSTER_CONFIG.items():
            self._leaders[cluster_id] = config["initial_leader"]
    
    def get_leader(self, cluster_id: str) -> str:
        return self._leaders.get(cluster_id, CLUSTER_CONFIG[cluster_id]["initial_leader"])
    
    def set_leader(self, cluster_id: str, node_id: str):
        self._leaders[cluster_id] = node_id
    
    def reset(self):
        for cluster_id, config in CLUSTER_CONFIG.items():
            self._leaders[cluster_id] = config["initial_leader"]

@dataclass
class BenchmarkConfig:
    """Benchmark workload configuration (YCSB/SmallBank-like)."""
    total_transactions: int = 200
    read_write_ratio: float = 0.8     # 0-1: fraction of read-write txns
    cross_shard_ratio: float = 0.1    # 0-1: fraction of cross-shard
    skewness: float = 0.0             # 0-1: Zipfian skewness
    
    def to_dict(self) -> Dict:
        return {
            "total_transactions": self.total_transactions,
            "read_write_ratio": self.read_write_ratio,
            "cross_shard_ratio": self.cross_shard_ratio,
            "skewness": self.skewness
        }

@dataclass
class PerformanceMetrics:
    total_transactions: int = 0
    successful_transactions: int = 0
    failed_transactions: int = 0
    aborted_transactions: int = 0
    total_latency_ms: float = 0.0
    min_latency_ms: float = float('inf')
    max_latency_ms: float = 0.0
    start_time: float = 0.0
    end_time: float = 0.0
    intra_shard_count: int = 0
    cross_shard_count: int = 0
    read_only_count: int = 0
    
    def add_latency(self, latency_ms: float, txn_type: TxnType = TxnType.INTRA_SHARD):
        self.total_transactions += 1
        self.total_latency_ms += latency_ms
        self.min_latency_ms = min(self.min_latency_ms, latency_ms)
        self.max_latency_ms = max(self.max_latency_ms, latency_ms)
        if txn_type == TxnType.INTRA_SHARD:
            self.intra_shard_count += 1
        elif txn_type == TxnType.CROSS_SHARD:
            self.cross_shard_count += 1
        else:
            self.read_only_count += 1
    
    @property
    def avg_latency_ms(self) -> float:
        return self.total_latency_ms / self.total_transactions if self.total_transactions else 0.0
    
    @property
    def throughput(self) -> float:
        duration = self.end_time - self.start_time
        return self.total_transactions / duration if duration > 0 else 0.0
    
    def summary(self) -> str:
        return (
            f"Performance Summary:\n"
            f"  Total: {self.total_transactions}, Success: {self.successful_transactions}, "
            f"Failed: {self.failed_transactions}, Aborted: {self.aborted_transactions}\n"
            f"  Throughput: {self.throughput:.2f} txn/sec\n"
            f"  Latency: avg={self.avg_latency_ms:.2f}ms, min={self.min_latency_ms:.2f}ms, max={self.max_latency_ms:.2f}ms\n"
            f"  Types: intra={self.intra_shard_count}, cross={self.cross_shard_count}, read={self.read_only_count}"
        )
    
    def reset(self):
        self.total_transactions = 0
        self.successful_transactions = 0
        self.failed_transactions = 0
        self.aborted_transactions = 0
        self.total_latency_ms = 0.0
        self.min_latency_ms = float('inf')
        self.max_latency_ms = 0.0
        self.start_time = 0.0
        self.end_time = 0.0
        self.intra_shard_count = 0
        self.cross_shard_count = 0
        self.read_only_count = 0