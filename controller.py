import asyncio
import csv
import json
import logging
import argparse
import time
import random
import math
from collections import defaultdict
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Tuple, Set, Literal, Any

from common import (
    NODE_CONFIG, CLUSTER_CONFIG, CONTROLLER_HOST, CONTROLLER_PORT,
    INITIAL_SHARD_RANGES, TOTAL_DATA_ITEMS, INITIAL_BALANCE,
    MSG_REQUEST, MSG_REPLY, MSG_CONTROL, MSG_BALANCE_REQUEST, MSG_BALANCE_REPLY,
    TxnType, Transaction, Request,
    get_cluster_for_item, get_nodes_in_cluster, get_cluster_for_node,
    is_cross_shard, SHARD_MAP,
    BenchmarkConfig, PerformanceMetrics
)

logging.basicConfig(format="[CONTROLLER] %(message)s", level=logging.INFO)


SET_TIMEOUT_SEC = 60.0  
CLIENT_RETRY_MS = 2000

@dataclass
class TestItem:
    kind: Literal["transfer", "balance", "fail", "recover"]
    sender: Optional[int] = None
    receiver: Optional[int] = None
    amount: Optional[int] = None
    node_id: Optional[str] = None  # For F(n) and R(n)
    
    def __str__(self):
        if self.kind == "transfer":
            return f"({self.sender}, {self.receiver}, {self.amount})"
        elif self.kind == "balance":
            return f"({self.sender})"
        elif self.kind == "fail":
            return f"F({self.node_id})"
        elif self.kind == "recover":
            return f"R({self.node_id})"
        return "unknown"

@dataclass
class TestSet:
    set_number: int
    live_nodes: List[str]
    items: List[TestItem]

def parse_csv(path: str) -> List[TestSet]:
    sets = []
    current = None
    
    try:
        with open(path, newline='') as f:
            reader = csv.reader(f)
            next(reader, None)  # Skip header
            
            for row in reader:
                while len(row) < 3:
                    row.append("")
                
                set_num, txn_str, live_str = [x.strip() for x in row]
                
                # New set
                if set_num:
                    if current:
                        sets.append(current)
                    
                    # Parse live nodes
                    nodes = []
                    if live_str:
                        live_str = live_str.strip("[]")
                        nodes = [n.strip() for n in live_str.split(",") if n.strip()]
                    
                    current = TestSet(int(set_num), nodes, [])
                
                # Parse transaction/command
                if txn_str and current:
                    item = parse_item(txn_str)
                    if item:
                        current.items.append(item)
        
        if current:
            sets.append(current)
    
    except FileNotFoundError:
        logging.error(f"File not found: {path}")
        exit(1)
    
    return sets

def parse_item(s: str) -> Optional[TestItem]:
    s = s.strip()
    
    if s.startswith("F(") and s.endswith(")"):
        node_id = s[2:-1].strip()
        return TestItem("fail", node_id=node_id)
    
    if s.startswith("R(") and s.endswith(")"):
        node_id = s[2:-1].strip()
        return TestItem("recover", node_id=node_id)
    
    if s.startswith("(") and s.endswith(")"):
        parts = [p.strip() for p in s[1:-1].split(",")]
        
        if len(parts) == 1:
            return TestItem("balance", sender=int(parts[0]))
        elif len(parts) == 3:
            return TestItem("transfer", 
                          sender=int(parts[0]), 
                          receiver=int(parts[1]), 
                          amount=int(parts[2]))
    
    return None


class ConnectionPool:
    """Maintains persistent TCP connections to nodes for better performance."""
    
    def __init__(self):
        self._writers: Dict[str, asyncio.StreamWriter] = {}
        self._readers: Dict[str, asyncio.StreamReader] = {}
        self._locks: Dict[str, asyncio.Lock] = {}
        
    def _get_lock(self, node_id: str) -> asyncio.Lock:
        """Get or create a lock for a node."""
        if node_id not in self._locks:
            self._locks[node_id] = asyncio.Lock()
        return self._locks[node_id]
    
    async def send(self, node_id: str, msg: Dict) -> bool:
        """Send a message to a node using pooled connection."""
        if node_id not in NODE_CONFIG:
            return False
        
        lock = self._get_lock(node_id)
        async with lock:
            try:
                # Check if we have a valid connection
                writer = self._writers.get(node_id)
                if writer is None or writer.is_closing():
                    # Create new connection
                    reader, writer = await asyncio.wait_for(
                        asyncio.open_connection(
                            NODE_CONFIG[node_id]["host"],
                            NODE_CONFIG[node_id]["port"]
                        ),
                        timeout=2.0
                    )
                    self._writers[node_id] = writer
                    self._readers[node_id] = reader
                
                # Send message
                writer = self._writers[node_id]
                writer.write((json.dumps(msg) + "\n").encode())
                await writer.drain()
                return True
                
            except Exception as e:
                # Connection failed, remove from pool
                await self._close_connection(node_id)
                logging.warning(f"ConnectionPool: Failed to send to {node_id}: {e}")
                return False
    
    async def _close_connection(self, node_id: str):
        """Close and remove a connection from the pool."""
        writer = self._writers.pop(node_id, None)
        self._readers.pop(node_id, None)
        if writer and not writer.is_closing():
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
    
    async def close_all(self):
        """Close all connections in the pool."""
        for node_id in list(self._writers.keys()):
            await self._close_connection(node_id)
    
    async def reconnect_all(self, node_ids: List[str]):
        """Close existing connections and allow fresh reconnection."""
        await self.close_all()


class ClientManager:
    """Manages client state and transaction sending."""
    
    def __init__(self):
        self.ts_map: Dict[str, int] = defaultdict(int)  # client_id -> timestamp
        self.pending: Dict[str, Tuple[int, asyncio.Task, float]] = {}  # request_id -> (ts, retry_task, start_time)
        self.completed: Set[str] = set()
        
        # Connection pool for efficient messaging
        self.conn_pool = ConnectionPool()
        
        # Track cluster leaders
        self.cluster_leaders: Dict[str, str] = {}
        for cid, config in CLUSTER_CONFIG.items():
            self.cluster_leaders[cid] = config["initial_leader"]
        
        # Performance tracking
        self.metrics = PerformanceMetrics()
        
        # Leader query responses
        self.leader_query_responses: Dict[str, Dict] = {}
        
        # Resharding balance responses
        self.reshard_balances: Dict[int, int] = {}  # account_id -> balance
        
        # View history responses from nodes
        self.view_data: Dict[str, Dict] = {}  # node_id -> view data
        
        # DB data responses from nodes
        self.db_data: Dict[str, Dict] = {}  # node_id -> balance data
        
        # Balance query responses from nodes
        self.balance_query_data: Dict[str, Dict] = {}  # node_id -> balance
    
    def get_client_id(self, sender: int) -> str:
        """Generate client ID for a sender account."""
        return f"client_{sender}"
    
    def get_target_cluster(self, sender: int) -> str:
        """Get cluster responsible for sender."""
        return get_cluster_for_item(sender)
    
    def get_target_leader(self, sender: int) -> str:
        """Get leader of cluster responsible for sender."""
        cluster = self.get_target_cluster(sender)
        return self.cluster_leaders.get(cluster, CLUSTER_CONFIG[cluster]["initial_leader"])
    
    async def send_transfer(self, item: TestItem, live_nodes: List[str]) -> str:
        """Send a transfer transaction. Returns request_id."""
        client_id = self.get_client_id(item.sender)
        self.ts_map[client_id] += 1
        ts = self.ts_map[client_id]
        request_id = f"{client_id}:{ts}"
        
        txn_type = TxnType.INTRA_SHARD
        if not SHARD_MAP.is_intra_shard(item.sender, item.receiver):
            txn_type = TxnType.CROSS_SHARD
        
        msg = {
            "type": MSG_REQUEST,
            "src": client_id,
            "dst": self.get_target_leader(item.sender),
            "client_id": client_id,
            "timestamp": ts,
            "request_id": request_id,
            "txn": {
                "sender": item.sender,
                "receiver": item.receiver,
                "amount": item.amount,
                "txn_type": txn_type.value
            }
        }
        
        start_time = time.time()
        retry_task = asyncio.create_task(self.retry_transfer(client_id, ts, msg, live_nodes))
        self.pending[request_id] = (ts, retry_task, start_time, txn_type)
        
        target = self.get_target_leader(item.sender)
        if target in live_nodes:
            await self.send(target, msg)
        else:
            cluster = self.get_target_cluster(item.sender)
            for node in get_nodes_in_cluster(cluster):
                if node in live_nodes:
                    msg["dst"] = node
                    await self.send(node, msg)
                    break
        
        return request_id
    
    async def retry_transfer(self, client_id: str, ts: int, msg: Dict, live_nodes: List[str]):
        max_retries = 12  # Increased for lock conflicts
        retries = 0
        while retries < max_retries:
            try:
                # Add jitter to break lock conflict cycles
                jitter = random.uniform(0, 1.0)
                await asyncio.sleep((CLIENT_RETRY_MS / 1000.0) + jitter)
                request_id = f"{client_id}:{ts}"
                if request_id not in self.pending:
                    break
                
                retries += 1
                # Broadcast to all live nodes in target cluster
                cluster = self.get_target_cluster(msg["txn"]["sender"])
                sent = False
                for node in get_nodes_in_cluster(cluster):
                    if node in live_nodes:
                        msg["dst"] = node
                        await self.send(node, msg)
                        sent = True
                
                if not sent:
                    logging.warning(f"No live nodes in cluster {cluster} for transfer")
                    break
            except asyncio.CancelledError:
                break
        
        # If still pending after max retries, complete with timeout
        request_id = f"{client_id}:{ts}"
        if request_id in self.pending:
            _, task, start_time, txn_type = self.pending[request_id]
            latency = (time.time() - start_time) * 1000
            logging.warning(f"Transfer {request_id} timed out after {latency:.1f}ms")
            self.metrics.add_latency(latency, txn_type)
            self.metrics.failed_transactions += 1
            del self.pending[request_id]
    
    async def send_balance(self, item: TestItem, live_nodes: List[str]) -> str:
        client_id = self.get_client_id(item.sender)
        self.ts_map[client_id] += 1
        ts = self.ts_map[client_id]
        request_id = f"{client_id}:{ts}:bal"
        
        msg = {
            "type": MSG_BALANCE_REQUEST,
            "src": client_id,
            "dst": self.get_target_leader(item.sender),
            "client_id": client_id,
            "timestamp": ts,
            "account_id": item.sender,
            "request_id": request_id
        }
        
        start_time = time.time()
        retry_task = asyncio.create_task(self.retry_balance(client_id, ts, msg, live_nodes))
        self.pending[request_id] = (ts, retry_task, start_time, TxnType.READ_ONLY)
        
        target = self.get_target_leader(item.sender)
        if target in live_nodes:
            await self.send(target, msg)
        else:
            cluster = self.get_target_cluster(item.sender)
            for node in get_nodes_in_cluster(cluster):
                if node in live_nodes:
                    msg["dst"] = node
                    await self.send(node, msg)
                    break
        
        return request_id
    
    async def retry_balance(self, client_id: str, ts: int, msg: Dict, live_nodes: List[str]):
        """Retry balance query if no reply received."""
        max_retries = 5
        retries = 0
        while retries < max_retries:
            try:
                await asyncio.sleep(CLIENT_RETRY_MS / 1000.0)
                request_id = f"{client_id}:{ts}:bal"
                if request_id not in self.pending:
                    break
                
                retries += 1
                cluster = self.get_target_cluster(msg["account_id"])
                sent = False
                for node in get_nodes_in_cluster(cluster):
                    if node in live_nodes:
                        msg["dst"] = node
                        await self.send(node, msg)
                        sent = True
                
                if not sent:
                    # No live nodes in cluster, stop retrying
                    logging.warning(f"No live nodes in cluster {cluster} for balance query")
                    break
            except asyncio.CancelledError:
                break
        
        request_id = f"{client_id}:{ts}:bal"
        if request_id in self.pending:
            _, task, start_time, txn_type = self.pending[request_id]
            latency = (time.time() - start_time) * 1000
            logging.warning(f"Balance query {request_id} timed out after {latency:.1f}ms")
            self.metrics.add_latency(latency, TxnType.READ_ONLY)
            self.metrics.failed_transactions += 1
            del self.pending[request_id]
    
    def on_reply(self, msg: Dict):
        """Handle reply from node."""
        client_id = msg.get("client_id", "")
        ts = msg.get("timestamp", 0)
        result = msg.get("result", "")
        src = msg.get("src", "")
        
        # Update leader tracking
        if src and result in ["success", "failure", "aborted", "locked"]:
            cluster = get_cluster_for_node(src)
            if self.cluster_leaders.get(cluster) != src:
                logging.info(f"Leader update: {cluster} -> {src}")
                self.cluster_leaders[cluster] = src
        
        # Find matching pending request
        found = False
        for request_id in list(self.pending.keys()):
            if request_id.startswith(f"{client_id}:") and "bal" not in request_id:
                pending_ts, task, start_time, txn_type = self.pending[request_id]
                if ts >= pending_ts:
                    found = True
                    if result == "locked":
                        logging.info(f"TRANSFER {request_id}: locked, will retry")
                        return  
                    
                    latency = (time.time() - start_time) * 1000
                    
                    self.metrics.add_latency(latency, txn_type)
                    if result == "success":
                        self.metrics.successful_transactions += 1
                    elif result == "failure":
                        self.metrics.failed_transactions += 1
                    elif result == "aborted":
                        self.metrics.aborted_transactions += 1
                    

                    account_id = client_id.replace("client_", "")
                    logging.info(f"TRANSFER REPLY for {request_id}: {result} ({latency:.1f}ms) from {src}")
                    task.cancel()
                    del self.pending[request_id]
                    self.completed.add(request_id)
                    break
        
        if not found and result:
            pass  
    
    def on_balance_reply(self, msg: Dict):
        """Handle balance reply."""
        client_id = msg.get("client_id", "")
        ts = msg.get("timestamp", 0)
        balance = msg.get("balance", 0)
        result = msg.get("result", "success")
        src = msg.get("src", "")
        
        request_id = f"{client_id}:{ts}:bal"
        if request_id in self.pending:
            _, task, start_time, txn_type = self.pending[request_id]
            latency = (time.time() - start_time) * 1000
            
            self.metrics.add_latency(latency, TxnType.READ_ONLY)
            if result == "success":
                self.metrics.successful_transactions += 1
                account_id = client_id.replace("client_", "")
                logging.info(f"BALANCE REPLY: account {account_id} = {balance} ({latency:.1f}ms)")
            else:
                self.metrics.failed_transactions += 1
                logging.info(f"Balance query failed: {result}")
            
            task.cancel()
            del self.pending[request_id]
            self.completed.add(request_id)
    
    async def query_leaders(self, live_nodes: List[str]) -> Dict[str, str]:
        self.leader_query_responses.clear()
        
        for node in live_nodes:
            await self.send(node, {
                "type": MSG_CONTROL,
                "src": "ctl",
                "dst": node,
                "op": "QUERY_LEADER"
            })
        
        await asyncio.sleep(0.5)
        
        for node_id, response in self.leader_query_responses.items():
            if response.get("is_leader"):
                cluster_id = response.get("cluster_id")
                if cluster_id:
                    self.cluster_leaders[cluster_id] = node_id
        
        return self.cluster_leaders.copy()
    
    async def send(self, node_id: str, msg: Dict) -> bool:
        success = await self.conn_pool.send(node_id, msg)
        if not success:
            # Retry once after clearing connection
            await asyncio.sleep(0.1)
            success = await self.conn_pool.send(node_id, msg)
        return success
    
    def clear_pending(self):
        """Clear all pending requests."""
        for request_id, (ts, task, start_time, txn_type) in list(self.pending.items()):
            task.cancel()
        self.pending.clear()
    
    async def reset(self):
        """Reset client state for new test set."""
        self.clear_pending()
        self.ts_map.clear()
        self.completed.clear()
        self.metrics.reset()
        
        await self.conn_pool.close_all()
        
        # Reset to initial leaders
        for cid, config in CLUSTER_CONFIG.items():
            self.cluster_leaders[cid] = config["initial_leader"]

class BenchmarkGenerator:
    
    def __init__(self, config: BenchmarkConfig):
        self.config = config
        self.transactions: List[TestItem] = []
        
        self._zipf_sum = None
        self._zipf_cumulative = None
        if config.skewness > 0:
            self._precompute_zipfian(TOTAL_DATA_ITEMS)
    
    def _precompute_zipfian(self, n: int):
        theta = self.config.skewness
        
        # Compute Zipfian probabilities
        # P(k) = (1/k^theta) / sum(1/i^theta for all i)
        weights = [1.0 / (k ** theta) for k in range(1, n + 1)]
        self._zipf_sum = sum(weights)
        
        # Compute cumulative distribution for efficient sampling
        cumulative = 0.0
        self._zipf_cumulative = []
        for w in weights:
            cumulative += w / self._zipf_sum
            self._zipf_cumulative.append(cumulative)
    
    def _sample_zipfian(self, n: int) -> int:
        if self._zipf_cumulative is None or len(self._zipf_cumulative) != n:
            self._precompute_zipfian(n)
        
        # Binary search for the sampled rank
        u = random.random()
        left, right = 0, n - 1
        while left < right:
            mid = (left + right) // 2
            if self._zipf_cumulative[mid] < u:
                left = mid + 1
            else:
                right = mid
        
        return left + 1  # Convert 0-indexed to 1-indexed
    
    def _scramble(self, rank: int, n: int) -> int:
        FNV_PRIME = 16777619
        FNV_OFFSET = 2166136261
        
        h = FNV_OFFSET
        for byte in rank.to_bytes(4, 'little'):
            h ^= byte
            h = (h * FNV_PRIME) & 0xFFFFFFFF
        
        return (h % n) + 1
    
    def generate(self) -> List[TestItem]:
        """Generate transactions according to config."""
        self.transactions = []
        
        for _ in range(self.config.total_transactions):
            if random.random() < (1 - self.config.read_write_ratio):
                # Read-only transaction (balance query)
                account = self._sample_account()
                self.transactions.append(TestItem("balance", sender=account))
            else:
                # Read-write transaction (transfer)
                if random.random() < self.config.cross_shard_ratio:
                    # Cross-shard transfer
                    sender = self._sample_account()
                    sender_cluster = get_cluster_for_item(sender)
                    other_clusters = [c for c in CLUSTER_CONFIG.keys() if c != sender_cluster]
                    target_cluster = random.choice(other_clusters)
                    start, end = INITIAL_SHARD_RANGES[target_cluster]
                    receiver = self._sample_from_range(start, end)
                    amount = random.randint(1, 3)
                    self.transactions.append(TestItem("transfer", sender=sender, receiver=receiver, amount=amount))
                else:
                    # Intra-shard transfer
                    sender = self._sample_account()
                    sender_cluster = get_cluster_for_item(sender)
                    start, end = INITIAL_SHARD_RANGES[sender_cluster]
                    receiver = self._sample_from_range(start, end)
                    while receiver == sender:
                        receiver = self._sample_from_range(start, end)
                    amount = random.randint(1, 3)
                    self.transactions.append(TestItem("transfer", sender=sender, receiver=receiver, amount=amount))
        
        return self.transactions
    
    def _sample_account(self) -> int:
        if self.config.skewness <= 0:
            # Uniform distribution
            return random.randint(1, TOTAL_DATA_ITEMS)
        else:
            # Zipfian distribution with scrambling
            rank = self._sample_zipfian(TOTAL_DATA_ITEMS)
            return self._scramble(rank, TOTAL_DATA_ITEMS)
    
    def _sample_from_range(self, start: int, end: int) -> int:
        """Sample from a specific range with skewness."""
        n = end - start + 1
        
        if self.config.skewness <= 0:
            return random.randint(start, end)
        else:
            # For range sampling, use simple power law (faster than full Zipfian)
            # This maintains skew within the range
            theta = self.config.skewness
            u = random.random()
            # Inverse CDF of power law: rank = n * u^(1/(1-theta)) for theta < 1
            if theta >= 1:
                theta = 0.99  # Cap at 0.99 to avoid division by zero
            rank = int(n * (u ** (1 / (1 - theta))))
            rank = min(rank, n - 1)
            return start + rank

class ReshardingManager:
    """Manages resharding using hypergraph partitioning."""
    
    def __init__(self):
        self.transaction_history: List[Tuple[int, int]] = []  # (sender, receiver) pairs
        self.num_partitions = len(CLUSTER_CONFIG)
    
    def record_transaction(self, sender: int, receiver: int):
        """Record a transaction for resharding analysis."""
        self.transaction_history.append((sender, receiver))
    
    def compute_new_mapping(self, n_transactions: int = 1000) -> Dict[int, str]:
        """
        Compute new shard mapping using hypergraph partitioning.
        Goal: Minimize cross-shard transactions while keeping shards balanced.
        """
        recent = self.transaction_history[-n_transactions:] if len(self.transaction_history) > n_transactions else self.transaction_history
        
        if not recent:
            return {}
        
        # Build co-access graph
        co_access: Dict[int, Dict[int, int]] = defaultdict(lambda: defaultdict(int))
        access_count: Dict[int, int] = defaultdict(int)
        
        for sender, receiver in recent:
            co_access[sender][receiver] += 1
            co_access[receiver][sender] += 1
            access_count[sender] += 1
            access_count[receiver] += 1
        
        # Get all accounts that appeared in transactions
        hot_accounts = set(access_count.keys())
        
        # Initialize with CURRENT mapping (key fix!)
        new_mapping: Dict[int, str] = {}
        for account in hot_accounts:
            new_mapping[account] = SHARD_MAP.get_cluster(account)
        
        # Calculate current cross-shard score
        def calc_cross_shard_score(mapping: Dict[int, str]) -> int:
            score = 0
            for sender, receiver in recent:
                if mapping.get(sender) != mapping.get(receiver):
                    score += 1
            return score
        
        current_score = calc_cross_shard_score(new_mapping)
        
        # If already optimal (no cross-shard), return current mapping
        if current_score == 0:
            logging.info(f"Reshard: Already optimal (0 cross-shard transactions)")
            return new_mapping
        
        sorted_accounts = sorted(hot_accounts, key=lambda x: access_count[x], reverse=True)
        
        # Get current cluster sizes (for balancing)
        cluster_sizes: Dict[str, int] = {}
        for cluster in CLUSTER_CONFIG.keys():
            cluster_sizes[cluster] = len(SHARD_MAP.get_items_for_cluster(cluster))
        
        improved = True
        iterations = 0
        max_iterations = len(hot_accounts) * 2
        
        while improved and iterations < max_iterations:
            improved = False
            iterations += 1
            
            for account in sorted_accounts:
                current_cluster = new_mapping[account]
                best_cluster = current_cluster
                best_score = calc_cross_shard_score(new_mapping)
                
                # Try moving to each other cluster
                for cluster in CLUSTER_CONFIG.keys():
                    if cluster == current_cluster:
                        continue
                    
                    # Check balance constraint (don't make any cluster too large)
                    size_diff = cluster_sizes[cluster] - cluster_sizes[current_cluster]
                    if size_diff > 100:  # Don't move to much larger cluster
                        continue
                    
                    # Try the move
                    new_mapping[account] = cluster
                    new_score = calc_cross_shard_score(new_mapping)
                    
                    if new_score < best_score:
                        best_score = new_score
                        best_cluster = cluster
                    
                    # Revert
                    new_mapping[account] = current_cluster
                
                # Apply best move if it improves score
                if best_cluster != current_cluster:
                    new_mapping[account] = best_cluster
                    cluster_sizes[current_cluster] -= 1
                    cluster_sizes[best_cluster] += 1
                    improved = True
                    logging.info(f"Reshard: Moving {account} from {current_cluster} to {best_cluster} reduces cross-shard")
        
        final_score = calc_cross_shard_score(new_mapping)
        logging.info(f"Reshard: Reduced cross-shard from {current_score} to {final_score}")
        
        return new_mapping
    
    def compute_movements(self, new_mapping: Dict[int, str]) -> List[Tuple[int, str, str]]:
        movements = []
        
        for account_id, new_cluster in new_mapping.items():
            old_cluster = SHARD_MAP.get_cluster(account_id)
            if old_cluster != new_cluster:
                movements.append((account_id, old_cluster, new_cluster))
        
        return movements
    
    async def execute_reshard(self, movements: List[Tuple[int, str, str]], 
                              mgr: 'ClientManager', live_nodes: List[str]) -> int:
        moved_count = 0
        
        cluster_items: Dict[str, List[int]] = defaultdict(list)
        for account_id, old_cluster, new_cluster in movements:
            cluster_items[old_cluster].append(account_id)
        
        # Get balances from source clusters
        balances: Dict[int, int] = {}
        
        for cluster_id, account_ids in cluster_items.items():
            # Find leader of source cluster
            leader = mgr.cluster_leaders.get(cluster_id, CLUSTER_CONFIG[cluster_id]["initial_leader"])
            if leader not in live_nodes:
                # Find any live node in cluster
                for node in get_nodes_in_cluster(cluster_id):
                    if node in live_nodes:
                        leader = node
                        break
            
            # Query balances for these accounts
            for account_id in account_ids:
                await mgr.send(leader, {
                    "type": MSG_CONTROL,
                    "src": "ctl",
                    "dst": leader,
                    "op": "GET_BALANCE_FOR_RESHARD",
                    "account_id": account_id
                })
        
        # Wait for responses (increased timeout for many accounts)
        wait_time = max(1.0, len(movements) * 0.1)  # At least 1s, +0.1s per account
        await asyncio.sleep(wait_time)
        
        # Now get balances from the responses (stored in mgr.reshard_balances)
        balances = mgr.reshard_balances.copy()
        mgr.reshard_balances.clear()
        
        # Log retrieval results
        retrieved = len(balances)
        needed = len(movements)
        if retrieved < needed:
            missing = [acc for acc, _, _ in movements if acc not in balances]
            logging.warning(f"Reshard: Only got {retrieved}/{needed} balances. Missing: {missing[:10]}{'...' if len(missing) > 10 else ''}")
        
        # Execute movements
        for account_id, old_cluster, new_cluster in movements:
            if account_id not in balances:
                logging.warning(f"Reshard: Could not get balance for account {account_id}")
                continue
            
            balance = balances[account_id]
            
            # Find leaders
            old_leader = mgr.cluster_leaders.get(old_cluster, CLUSTER_CONFIG[old_cluster]["initial_leader"])
            new_leader = mgr.cluster_leaders.get(new_cluster, CLUSTER_CONFIG[new_cluster]["initial_leader"])
            
            # Ensure leaders are live
            if old_leader not in live_nodes:
                for node in get_nodes_in_cluster(old_cluster):
                    if node in live_nodes:
                        old_leader = node
                        break
            
            if new_leader not in live_nodes:
                for node in get_nodes_in_cluster(new_cluster):
                    if node in live_nodes:
                        new_leader = node
                        break
            
            update_targets = [node for node in get_nodes_in_cluster(new_cluster) if node in live_nodes]
            logging.info(f"Reshard: Sending UPDATE_DATA for {account_id} to {update_targets}")
            for node in update_targets:
                success = await mgr.send(node, {
                    "type": MSG_CONTROL,
                    "src": "ctl",
                    "dst": node,
                    "op": "UPDATE_DATA",
                    "account_id": account_id,
                    "balance": balance
                })
                if not success:
                    logging.error(f"Reshard: FAILED to send UPDATE_DATA to {node} for account {account_id}")
                await asyncio.sleep(0.05)  # Small delay between sends
            
            # Wait for UPDATEs to complete before DELETE
            await asyncio.sleep(0.2)
            
            delete_targets = [node for node in get_nodes_in_cluster(old_cluster) if node in live_nodes]
            logging.info(f"Reshard: Sending DELETE_DATA for {account_id} to {delete_targets}")
            for node in delete_targets:
                success = await mgr.send(node, {
                    "type": MSG_CONTROL,
                    "src": "ctl",
                    "dst": node,
                    "op": "DELETE_DATA",
                    "account_id": account_id
                })
                if not success:
                    logging.error(f"Reshard: FAILED to send DELETE_DATA to {node} for account {account_id}")
                await asyncio.sleep(0.05)  
            
            SHARD_MAP.update_mapping(account_id, new_cluster)
            
            moved_count += 1
            logging.info(f"Reshard: Moved account {account_id} ({balance}) from {old_cluster} to {new_cluster}")
            
            
            await asyncio.sleep(0.2)
        
        await asyncio.sleep(0.3)
        
        
        if movements:
            mapping_updates = {str(acc): new_clust for acc, _, new_clust in movements}
            all_nodes = list(NODE_CONFIG.keys())
            logging.info(f"Reshard: Sending SHARD_MAP updates ({len(mapping_updates)} accounts) to all nodes...")
            for node in all_nodes:
                if node in live_nodes:
                    success = await mgr.send(node, {
                        "type": MSG_CONTROL,
                        "src": "ctl",
                        "dst": node,
                        "op": "UPDATE_SHARD_MAP",
                        "mapping_updates": mapping_updates
                    })
                    if not success:
                        logging.error(f"Reshard: FAILED to send UPDATE_SHARD_MAP to {node}")
            await asyncio.sleep(0.2)
            logging.info(f"Reshard: SHARD_MAP updates sent to all nodes")
        
        return moved_count
    
    def reset(self):
        """Reset resharding state for new test set (keeps SHARD_MAP persistent)."""
        if self.transaction_history:
            logging.info(f"Clearing {len(self.transaction_history)} transactions from history")
        self.transaction_history.clear()
    
    def reset_full(self):
        """Full reset including SHARD_MAP (for complete system reset)."""
        self.transaction_history.clear()
        SHARD_MAP.reset()

async def run_controller(csv_path: str, run_sets: Optional[Set[int]] = None, skip_sets: Optional[Set[int]] = None):
    if skip_sets is None:
        skip_sets = set()
    
    mgr = ClientManager()
    reshard_mgr = ReshardingManager()
    
    # Message handler
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                
                try:
                    msg = json.loads(line)
                    mtype = msg.get("type")
                    
                    if mtype == MSG_REPLY:
                        mgr.on_reply(msg)
                    elif mtype == MSG_BALANCE_REPLY:
                        mgr.on_balance_reply(msg)
                    elif mtype == MSG_CONTROL:
                        op = msg.get("op")
                        if op == "QUERY_LEADER_RESPONSE":
                            mgr.leader_query_responses[msg["src"]] = msg
                        elif op == "BALANCE_FOR_RESHARD":
                            # Store balance for resharding
                            account_id = msg.get("account_id")
                            balance = msg.get("balance")
                            if account_id is not None and balance is not None:
                                mgr.reshard_balances[account_id] = balance
                        elif op == "DATA_FOR_RESHARD":
                            # Handle reshard data (not implemented for now)
                            pass
                        elif op == "VIEW_DATA":
                            # Store view history from node
                            node_id = msg.get("node_id")
                            if node_id:
                                mgr.view_data[node_id] = msg
                        elif op == "DB_DATA":
                            # Store DB data from node
                            node_id = msg.get("node_id")
                            if node_id:
                                mgr.db_data[node_id] = msg
                        elif op == "BALANCE_DATA":
                            # Store balance query result from node
                            node_id = msg.get("node_id")
                            if node_id:
                                logging.debug(f"Received BALANCE_DATA from {node_id}: balance={msg.get('balance')}")
                                mgr.balance_query_data[node_id] = msg
                except json.JSONDecodeError:
                    pass
        except Exception:
            pass
        finally:
            writer.close()
    
    # Start server
    server = await asyncio.start_server(handler, CONTROLLER_HOST, CONTROLLER_PORT)
    logging.info(f"Controller started on {CONTROLLER_HOST}:{CONTROLLER_PORT}")
    
    # Parse test cases
    test_sets = parse_csv(csv_path)
    logging.info(f"Loaded {len(test_sets)} test sets")
    
    all_nodes = list(NODE_CONFIG.keys())
    
    async with server:
        print(f"\n{'='*60}")
        print("System Ready - Initial Setup")
        print(f"{'='*60}")
        
        # Compute mapping updates (accounts that differ from initial shard ranges)
        initial_mapping_updates = {}
        for account_id, current_cluster in SHARD_MAP.get_full_mapping().items():
            if 1 <= account_id <= 3000:
                initial_cluster = "C1"
            elif 3001 <= account_id <= 6000:
                initial_cluster = "C2"
            else:
                initial_cluster = "C3"
            if current_cluster != initial_cluster:
                initial_mapping_updates[str(account_id)] = current_cluster
        
        # Reset and initialize all nodes with current shard mapping
        logging.info("Initializing all nodes...")
        for node in all_nodes:
            cluster = get_cluster_for_node(node)
            shard_accounts = SHARD_MAP.get_items_for_cluster(cluster)
            await mgr.send(node, {
                "type": MSG_CONTROL,
                "src": "ctl",
                "dst": node,
                "op": "RESET",
                "shard_accounts": shard_accounts,
                "mapping_updates": initial_mapping_updates
            })
        await asyncio.sleep(0.5)
        
        # Set all nodes live
        for node in all_nodes:
            await mgr.send(node, {
                "type": MSG_CONTROL,
                "src": "ctl",
                "dst": node,
                "op": "SET_LIVE",
                "live": True
            })
        await asyncio.sleep(0.5)
        
        # Query leaders
        await mgr.query_leaders(all_nodes)
        
        print("\nYou can run commands now before starting test sets:")
        print("  benchmark N RW CS SK - Run benchmark (N txns, RW%, CS%, skew 0-1)")
        print("  PrintDB              - Print all balances")
        print("  Performance          - Print performance metrics")
        print("  (empty line)         - Continue to test sets")
        print()
        
        while True:
            cmd = await asyncio.to_thread(input, "> ")
            cmd = cmd.strip()
            
            if not cmd:
                break
            
            await handle_command(cmd, mgr, reshard_mgr, all_nodes, all_nodes)
        
        for tset in test_sets:
            set_num = tset.set_number
            
            # Check if this set should be skipped
            if run_sets is not None and set_num not in run_sets:
                logging.info(f"Skipping Set {set_num} (not in --set list)")
                continue
            
            if set_num in skip_sets:
                logging.info(f"Skipping Set {set_num} (in --skip list)")
                continue
            
            print(f"\n{'='*60}")
            print(f"Set {set_num} (Live: {tset.live_nodes})")
            print(f"Items: {len(tset.items)}")
            print(f"{'='*60}")
            
            # Interactive prompt with skip option
            user_input = await asyncio.to_thread(input, "Press Enter to start, 's' to skip, 'q' to quit: ")
            user_input = user_input.strip().lower()
            
            if user_input == 's':
                logging.info(f"Skipping Set {set_num} (user requested)")
                continue
            elif user_input == 'q':
                logging.info("Quitting...")
                break
            
            # Reset state
            await mgr.reset()
            reshard_mgr.reset()  # Clear transaction history for new set
            mgr.metrics.start_time = time.time()
            
            # Reset all nodes with current shard mapping
            live = tset.live_nodes if tset.live_nodes else all_nodes
            
            mapping_updates = {}
            for account_id, current_cluster in SHARD_MAP.get_full_mapping().items():
                # Determine what the initial cluster would be
                if 1 <= account_id <= 3000:
                    initial_cluster = "C1"
                elif 3001 <= account_id <= 6000:
                    initial_cluster = "C2"
                else:
                    initial_cluster = "C3"
                
                if current_cluster != initial_cluster:
                    mapping_updates[str(account_id)] = current_cluster
            
            for node in all_nodes:
                cluster = get_cluster_for_node(node)
                shard_accounts = SHARD_MAP.get_items_for_cluster(cluster)
                await mgr.send(node, {
                    "type": MSG_CONTROL,
                    "src": "ctl",
                    "dst": node,
                    "op": "RESET",
                    "shard_accounts": shard_accounts,
                    "mapping_updates": mapping_updates  # Include mapping updates for resharded accounts
                })
            
            await asyncio.sleep(0.5)
            
            # Set live status
            for node in all_nodes:
                await mgr.send(node, {
                    "type": MSG_CONTROL,
                    "src": "ctl",
                    "dst": node,
                    "op": "SET_LIVE",
                    "live": node in live
                })
            
            await asyncio.sleep(0.5)
            
            # Query leaders
            await mgr.query_leaders(live)
            
            # Track which nodes are currently failed (within this set)
            failed_in_set: Set[str] = set()
            for node in all_nodes:
                if node not in live:
                    failed_in_set.add(node)
            
            pending_before_fr: List[str] = []  # Track pending request IDs before F/R
            
            for idx, item in enumerate(tset.items):
                if item.kind == "fail":
                    cluster = get_cluster_for_node(item.node_id)
                    wait_start = time.time()
                    while mgr.pending and (time.time() - wait_start) < 3.0:
                        await asyncio.sleep(0.1)
                    
                    # Fail node
                    logging.info(f"Failing node {item.node_id}")
                    await mgr.send(item.node_id, {
                        "type": MSG_CONTROL,
                        "src": "ctl",
                        "dst": item.node_id,
                        "op": "LF"
                    })
                    failed_in_set.add(item.node_id)
                    
                    # Update live nodes list
                    current_live = [n for n in live if n not in failed_in_set]
                    
                    # Wait for election if leader failed
                    if item.node_id == mgr.cluster_leaders.get(cluster):
                        await asyncio.sleep(2.0)  # Wait for election
                        await mgr.query_leaders(current_live)
                    else:
                        await asyncio.sleep(0.3)
                
                elif item.kind == "recover":
                    wait_start = time.time()
                    while mgr.pending and (time.time() - wait_start) < 2.0:
                        await asyncio.sleep(0.1)
                    
                    logging.info(f"Recovering node {item.node_id}")
                    await mgr.send(item.node_id, {
                        "type": MSG_CONTROL,
                        "src": "ctl",
                        "dst": item.node_id,
                        "op": "RECOVER"
                    })
                    failed_in_set.discard(item.node_id)
                    await asyncio.sleep(0.5)
                    
                    current_live = [n for n in live if n not in failed_in_set]
                    await mgr.query_leaders(current_live)
                
                elif item.kind == "transfer":
                    current_live = [n for n in live if n not in failed_in_set]
                    
                    client_id = mgr.get_client_id(item.sender)
                    wait_start = time.time()
                    while any(rid.startswith(f"{client_id}:") and "bal" not in rid for rid in mgr.pending):
                        await asyncio.sleep(0.05)
                        if time.time() - wait_start > 5.0:
                            logging.warning(f"Timeout waiting for client {client_id}, proceeding anyway")
                            break
                    
                    logging.info(f"Sending transfer: {item}")
                    await mgr.send_transfer(item, current_live)
                    
                    # Record for resharding
                    reshard_mgr.record_transaction(item.sender, item.receiver)
                
                elif item.kind == "balance":
                    current_live = [n for n in live if n not in failed_in_set]
                    
                    client_id = mgr.get_client_id(item.sender)
                    wait_start = time.time()
                    while any(rid.startswith(f"{client_id}:") and "bal" not in rid for rid in mgr.pending):
                        await asyncio.sleep(0.05)
                        if time.time() - wait_start > 3.0:
                            break
                    
                    logging.info(f"Sending balance query: {item}")
                    await mgr.send_balance(item, current_live)
            
            # Wait for completion
            start_wait = time.time()
            last_progress = time.time()
            logging.info(f"Waiting for {len(mgr.pending)} transactions to complete...")
            while mgr.pending:
                await asyncio.sleep(0.1)
                
                # Print progress every 5 seconds
                if time.time() - last_progress > 5.0:
                    pending_count = len(mgr.pending)
                    logging.info(f"Still waiting for {pending_count} pending transactions...")
                    if pending_count <= 10:
                        for rid in list(mgr.pending.keys())[:10]:
                            logging.info(f"  Pending: {rid}")
                    last_progress = time.time()
                
                if time.time() - start_wait > SET_TIMEOUT_SEC:
                    logging.warning(f"Set {set_num} TIMED OUT with {len(mgr.pending)} pending")
                    for rid in list(mgr.pending.keys())[:20]:
                        logging.warning(f"  Stuck: {rid}")
                    mgr.clear_pending()
                    break
            
            logging.info(f"All {len(mgr.completed)} transactions completed")
            
            mgr.metrics.end_time = time.time()
            
            # Pause timers
            for node in all_nodes:
                await mgr.send(node, {
                    "type": MSG_CONTROL,
                    "src": "ctl",
                    "dst": node,
                    "op": "PAUSE_TIMERS"
                })
            
            logging.info(f"Set {set_num} complete")
            
            # Interactive commands
            print("\nCommands:")
            print("  PrintDB              - Print modified balances (all nodes)")
            print("  PrintBalance(x)      - Print balance of account x")
            print("  PrintView            - Print NEW-VIEW messages")
            print("  PrintLog(n)          - Print log for node n")
            print("  Performance          - Print performance metrics")
            print("  PrintReshard         - Execute resharding")
            print("  PrintShardMap        - Show current shard mapping")
            print("  benchmark N RW CS SK - Run benchmark (N txns, RW%, CS%, skew 0-1)")
            print("  skip N               - Skip to set N (skipping intermediate sets)")
            print("  quit                 - Exit controller")
            print("  (empty line)         - Continue to next set")
            print()
            
            should_quit = False
            skip_to_set = None
            
            while True:
                cmd = await asyncio.to_thread(input, "> ")
                cmd = cmd.strip()
                
                if not cmd:
                    break
                
                if cmd.lower() == "quit":
                    should_quit = True
                    break
                
                if cmd.lower().startswith("skip "):
                    try:
                        skip_to_set = int(cmd.split()[1])
                        logging.info(f"Will skip to Set {skip_to_set}")
                        break
                    except (ValueError, IndexError):
                        print("Usage: skip N (where N is set number)")
                        continue
                
                await handle_command(cmd, mgr, reshard_mgr, all_nodes, live)
            
            if should_quit:
                logging.info("Quitting...")
                break
            
            # Skip sets if requested
            if skip_to_set is not None:
                # Add all sets between current and target to skip_sets
                for s in range(set_num + 1, skip_to_set):
                    skip_sets.add(s)

async def handle_command(cmd: str, mgr: ClientManager, reshard_mgr: ReshardingManager, 
                        all_nodes: List[str], live_nodes: List[str]):
    """Handle interactive command."""
    parts = cmd.split()
    
    if cmd.lower() == "printdb":
        print(f"\n=== PrintDB (modified items) ===")
        # Clear previous data
        mgr.db_data.clear()
        
        # Request DB data from all nodes
        for node in all_nodes:
            await mgr.send(node, {
                "type": MSG_CONTROL,
                "src": "ctl",
                "dst": node,
                "op": "GET_DB"
            })
            await asyncio.sleep(0.05)  # Small delay between sends
        
        # Wait for responses (increased)
        await asyncio.sleep(1.0)
        
        # Print results in order (by cluster, then node)
        for cluster_id in ["C1", "C2", "C3"]:
            for node in get_nodes_in_cluster(cluster_id):
                if node in mgr.db_data:
                    data = mgr.db_data[node]
                    balances = data.get("balances", {})
                    if balances:
                        items_str = ", ".join([f"{k}:{v}" for k, v in sorted(balances.items())])
                        print(f"{node}: {items_str}")
                    else:
                        print(f"{node}: (no modified items)")
                else:
                    print(f"{node}: (no response)")
        print("=== End PrintDB ===\n")
    
    elif cmd.lower().startswith("printbalance(") and cmd.endswith(")"):
        try:
            account_id = int(cmd[13:-1])
            cluster = get_cluster_for_item(account_id)
            cluster_nodes = get_nodes_in_cluster(cluster)
            
            # Clear previous data
            mgr.balance_query_data.clear()
            
            print(f"\n=== PrintBalance({account_id}) ===")
            print(f"Querying cluster {cluster}: {cluster_nodes}")
            
            for node in cluster_nodes:
                await mgr.send(node, {
                    "type": MSG_CONTROL,
                    "src": "ctl",
                    "dst": node,
                    "op": "GET_BALANCE",
                    "account_id": account_id
                })
                await asyncio.sleep(0.05)  # Small delay between sends
            
            # Wait for responses (increased)
            await asyncio.sleep(1.0)
            
            # Print results in order
            for node in cluster_nodes:
                if node in mgr.balance_query_data:
                    data = mgr.balance_query_data[node]
                    balance = data.get("balance")
                    print(f"{node} : {balance if balance is not None else 'N/A'}")
                else:
                    print(f"{node} : (no response)")
            print("=== End PrintBalance ===\n")
        except ValueError:
            print("Invalid account ID")
    
    elif cmd.lower() == "printview":
        print(f"\n=== PrintView ===")
        # Clear previous data
        mgr.view_data.clear()
        
        # Request view data from all nodes
        for node in all_nodes:
            await mgr.send(node, {
                "type": MSG_CONTROL,
                "src": "ctl",
                "dst": node,
                "op": "GET_VIEW"
            })
        
        # Wait for responses (increased)
        await asyncio.sleep(1.0)
        
        # Print results in order (by cluster, then node)
        for cluster_id in ["C1", "C2", "C3"]:
            for node in get_nodes_in_cluster(cluster_id):
                if node in mgr.view_data:
                    data = mgr.view_data[node]
                    history = data.get("view_history", [])
                    print(f"\n{'='*60}")
                    print(f"View History for {node} ({cluster_id})")
                    print(f"Total NEW-VIEW messages: {len(history)}")
                    print(f"{'='*60}")
                    
                    for idx, nv in enumerate(history, 1):
                        print(f"\n--- NEW-VIEW #{idx} ---")
                        ballot = nv.get('ballot', [0, '?'])
                        print(f"  Ballot: ({ballot[0]}, '{ballot[1]}')")
                        print(f"  Timestamp: {nv.get('timestamp', 'N/A')}")
                        print(f"  Entries: {len(nv.get('accept_log', []))}")
                        if 'acks_from' in nv:
                            print(f"  Role: LEADER (ACKs from: {nv['acks_from']})")
                        else:
                            print(f"  Role: FOLLOWER (from: {nv.get('received_from', 'unknown')})")
        
        print(f"\n=== End PrintView ===\n")
    
    elif cmd.lower().startswith("printlog(") and cmd.endswith(")"):
        node_id = cmd[9:-1]
        if node_id in NODE_CONFIG:
            await mgr.send(node_id, {
                "type": MSG_CONTROL,
                "src": "ctl",
                "dst": node_id,
                "op": "PRINT_LOG"
            })
            await asyncio.sleep(0.3)
        else:
            print(f"Unknown node: {node_id}")
    
    elif cmd.lower() == "performance":
        print(f"\n{mgr.metrics.summary()}\n")
    
    elif cmd.lower() == "printreshard":
        print("\n=== PrintReshard ===")
        print(f"Transaction history: {len(reshard_mgr.transaction_history)} transactions")
        print("Computing new mapping based on transaction history...")
        
        if not reshard_mgr.transaction_history:
            print("No transaction history available for resharding.")
            print("Run some transactions first, then try PrintReshard.")
            print("=== End PrintReshard ===\n")
            return
        
        new_mapping = reshard_mgr.compute_new_mapping()
        movements = reshard_mgr.compute_movements(new_mapping)
        
        if movements:
            print(f"Planned data movements ({len(movements)} items):")
            for account_id, old_cluster, new_cluster in sorted(movements):
                print(f"  Account {account_id}: {old_cluster} -> {new_cluster}")
            
            print("\nExecuting data movement...")
            moved = await reshard_mgr.execute_reshard(movements, mgr, live_nodes)
            print(f"Successfully moved {moved}/{len(movements)} items.")
        else:
            print("No data movements needed - current sharding is optimal")
        
        print("=== End PrintReshard ===\n")
    
    elif cmd.lower() == "printshardmap":
        print("\n=== PrintShardMap ===")
        for cluster_id in ["C1", "C2", "C3"]:
            accounts = SHARD_MAP.get_items_for_cluster(cluster_id)
            # Show summary: first 10, count of others
            if len(accounts) > 20:
                sample = sorted(accounts)[:10]
                print(f"{cluster_id}: {sample}... ({len(accounts)} total)")
            else:
                print(f"{cluster_id}: {sorted(accounts)}")
        print("=== End PrintShardMap ===\n")
    
    elif parts[0].lower() == "benchmark" and len(parts) >= 2:
        try:
            n_txns = int(parts[1])
            rw_ratio = float(parts[2]) / 100 if len(parts) > 2 else 0.8
            cs_ratio = float(parts[3]) / 100 if len(parts) > 3 else 0.1
            skew = float(parts[4]) if len(parts) > 4 else 0.0
            
            config = BenchmarkConfig(
                total_transactions=n_txns,
                read_write_ratio=rw_ratio,
                cross_shard_ratio=cs_ratio,
                skewness=skew
            )
            
            print(f"\nRunning benchmark: {n_txns} txns, {rw_ratio*100}% RW, {cs_ratio*100}% cross-shard, skew={skew}")
            
            gen = BenchmarkGenerator(config)
            items = gen.generate()
            
            mgr.metrics.reset()
            mgr.metrics.start_time = time.time()
            
            for item in items:
                if item.kind == "transfer":
                    client_id = mgr.get_client_id(item.sender)
                    while any(rid.startswith(f"{client_id}:") for rid in mgr.pending):
                        await asyncio.sleep(0.01)
                    await mgr.send_transfer(item, live_nodes)
                    reshard_mgr.record_transaction(item.sender, item.receiver)
                elif item.kind == "balance":
                    client_id = mgr.get_client_id(item.sender)
                    while any(rid.startswith(f"{client_id}:") for rid in mgr.pending):
                        await asyncio.sleep(0.01)
                    await mgr.send_balance(item, live_nodes)
            
            # Wait for completion
            start_wait = time.time()
            while mgr.pending:
                await asyncio.sleep(0.1)
                if time.time() - start_wait > 60:
                    logging.warning("Benchmark TIMED OUT")
                    mgr.clear_pending()
                    break
            
            mgr.metrics.end_time = time.time()
            print(f"\n{mgr.metrics.summary()}\n")
            
        except (ValueError, IndexError) as e:
            print(f"Usage: benchmark N [RW%] [CS%] [skew]")
            print(f"  N     = Number of transactions")
            print(f"  RW%   = Read-write percentage (0-100), default 80")
            print(f"  CS%   = Cross-shard percentage of RW (0-100), default 10")
            print(f"  skew  = Zipfian skewness (0-1), default 0")
            print(f"         0 = uniform, 0.99 = highly skewed (YCSB default)")
            print(f"Examples:")
            print(f"  benchmark 200              - 200 txns, default params")
            print(f"  benchmark 200 80 10 0      - 200 txns, 80% RW, 10% CS, uniform")
            print(f"  benchmark 200 80 10 0.99   - 200 txns with hotspots")
    
    else:
        print("Unknown command")


def parse_set_range(value: str) -> Set[int]:
    """Parse set numbers from string like '1', '1,3,5', '1-5', or '1-3,7,9-10'."""
    result = set()
    if not value:
        return result
    
    for part in value.split(','):
        part = part.strip()
        if '-' in part:
            start, end = part.split('-', 1)
            result.update(range(int(start), int(end) + 1))
        else:
            result.add(int(part))
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Distributed Transaction Controller")
    parser.add_argument("--csv", default="testcases.csv", help="Test cases CSV file")
    parser.add_argument("--set", "-s", dest="run_sets", default="", 
                        help="Run only specific sets (e.g., '5', '1,3,5', '1-5', '1-3,7')")
    parser.add_argument("--skip", dest="skip_sets", default="",
                        help="Skip specific sets (e.g., '2,4', '1-3')")
    args = parser.parse_args()
    
    run_sets = parse_set_range(args.run_sets) if args.run_sets else None
    skip_sets = parse_set_range(args.skip_sets) if args.skip_sets else set()
    
    try:
        asyncio.run(run_controller(args.csv, run_sets, skip_sets))
    except KeyboardInterrupt:
        pass