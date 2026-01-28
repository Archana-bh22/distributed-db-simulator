
import argparse
import asyncio
import json
import logging
import sqlite3
import os
import time
import random
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Any, Tuple
from datetime import datetime
from collections import defaultdict

from common import (
    NODE_CONFIG, CLUSTER_CONFIG, CONTROLLER_HOST, CONTROLLER_PORT,
    LEADER_TIMEOUT_MS, PREPARE_SUPPRESSION_MS, HEARTBEAT_INTERVAL_MS,
    TPC_TIMEOUT_MS, TPC_RETRY_INTERVAL_MS, TOTAL_DATA_ITEMS, INITIAL_BALANCE,
    MSG_REQUEST, MSG_REPLY, MSG_PREPARE, MSG_ACK, MSG_ACCEPT, MSG_ACCEPTED,
    MSG_COMMIT, MSG_NEW_VIEW, MSG_HEARTBEAT, MSG_CONTROL,
    MSG_2PC_PREPARE, MSG_2PC_PREPARED, MSG_2PC_ABORT, MSG_2PC_COMMIT, MSG_2PC_ACK,
    MSG_BALANCE_REQUEST, MSG_BALANCE_REPLY, MSG_LEADER_ANNOUNCE,
    AcceptFlag, TxnState, TxnType,
    Ballot, Transaction, Request, Reply, LogEntry, WALEntry, CrossShardTxn,
    get_cluster_for_node, get_nodes_in_cluster, get_cluster_for_item,
    get_quorum_size, get_other_clusters, is_cross_shard,
    base_msg, make_reply, make_balance_reply, generate_txn_id,
    SHARD_MAP, ClusterLeaderTracker, INITIAL_SHARD_RANGES
)

logging.basicConfig(format="[%(name)s] %(message)s", level=logging.INFO)

class DatabaseManager:
    
    def __init__(self, node_id: str, cluster_id: str, db_dir: str = "db"):
        self.node_id = node_id
        self.cluster_id = cluster_id
        os.makedirs(db_dir, exist_ok=True)
        self.db_path = os.path.join(db_dir, f"{node_id}_data.db")
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._init_schema()
        self._shard_accounts: Optional[List[int]] = None  
    
    def _init_schema(self):
        cursor = self.conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS balances (
                account_id INTEGER PRIMARY KEY,
                balance INTEGER NOT NULL
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS wal (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                txn_id TEXT NOT NULL,
                account_id INTEGER NOT NULL,
                before_balance INTEGER NOT NULL,
                after_balance INTEGER NOT NULL,
                timestamp REAL NOT NULL
            )
        ''')
       
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS modified_items (
                account_id INTEGER PRIMARY KEY
            )
        ''')
        self.conn.commit()
    
    def set_shard_accounts(self, accounts: List[int]):
        """Set which accounts this cluster owns (from controller's SHARD_MAP)."""
        self._shard_accounts = accounts
    
    def init_balances(self):
        """Initialize balances for accounts in this cluster's shard."""
        cursor = self.conn.cursor()
        if self._shard_accounts is not None:
           
            for account_id in self._shard_accounts:
                cursor.execute(
                    'INSERT OR IGNORE INTO balances (account_id, balance) VALUES (?, ?)',
                    (account_id, INITIAL_BALANCE)
                )
        else:
            
            start, end = INITIAL_SHARD_RANGES[self.cluster_id]
            for account_id in range(start, end + 1):
                cursor.execute(
                    'INSERT OR IGNORE INTO balances (account_id, balance) VALUES (?, ?)',
                    (account_id, INITIAL_BALANCE)
                )
        self.conn.commit()
    
    def get_balance(self, account_id: int) -> Optional[int]:
        cursor = self.conn.cursor()
        cursor.execute('SELECT balance FROM balances WHERE account_id = ?', (account_id,))
        row = cursor.fetchone()
        return row[0] if row else None
    
    def set_balance(self, account_id: int, balance: int, track_modified: bool = True):
        cursor = self.conn.cursor()
        cursor.execute(
            'INSERT OR REPLACE INTO balances (account_id, balance) VALUES (?, ?)',
            (account_id, balance)
        )
        if track_modified:
            cursor.execute(
                'INSERT OR IGNORE INTO modified_items (account_id) VALUES (?)',
                (account_id,)
            )
        self.conn.commit()
    
    def transfer(self, sender: int, receiver: int, amount: int) -> Tuple[bool, str]:
        
        cursor = self.conn.cursor()
        
       
        sender_cluster = get_cluster_for_item(sender)
        receiver_cluster = get_cluster_for_item(receiver)
        
        if sender_cluster == self.cluster_id:
            cursor.execute('SELECT balance FROM balances WHERE account_id = ?', (sender,))
            row = cursor.fetchone()
            if not row:
                return False, "sender_not_found"
            if row[0] < amount:
                return False, "insufficient_balance"
            
          
            cursor.execute(
                'UPDATE balances SET balance = balance - ? WHERE account_id = ?',
                (amount, sender)
            )
            cursor.execute(
                'INSERT OR IGNORE INTO modified_items (account_id) VALUES (?)',
                (sender,)
            )
        
        if receiver_cluster == self.cluster_id:
          
            cursor.execute(
                'UPDATE balances SET balance = balance + ? WHERE account_id = ?',
                (amount, receiver)
            )
            cursor.execute(
                'INSERT OR IGNORE INTO modified_items (account_id) VALUES (?)',
                (receiver,)
            )
        
        self.conn.commit()
        return True, "success"
    
    def debit_only(self, sender: int, amount: int) -> Tuple[bool, str]:
        cursor = self.conn.cursor()
        cursor.execute('SELECT balance FROM balances WHERE account_id = ?', (sender,))
        row = cursor.fetchone()
        if not row:
            return False, "sender_not_found"
        if row[0] < amount:
            return False, "insufficient_balance"
        
        cursor.execute(
            'UPDATE balances SET balance = balance - ? WHERE account_id = ?',
            (amount, sender)
        )
        cursor.execute(
            'INSERT OR IGNORE INTO modified_items (account_id) VALUES (?)',
            (sender,)
        )
        self.conn.commit()
        return True, "success"
    
    def credit_only(self, receiver: int, amount: int) -> Tuple[bool, str]:
        cursor = self.conn.cursor()
        cursor.execute(
            'UPDATE balances SET balance = balance + ? WHERE account_id = ?',
            (amount, receiver)
        )
        cursor.execute(
            'INSERT OR IGNORE INTO modified_items (account_id) VALUES (?)',
            (receiver,)
        )
        self.conn.commit()
        return True, "success"
    
  
    def write_wal(self, txn_id: str, account_id: int, before_bal: int, after_bal: int):
        cursor = self.conn.cursor()
        cursor.execute(
            'INSERT INTO wal (txn_id, account_id, before_balance, after_balance, timestamp) VALUES (?, ?, ?, ?, ?)',
            (txn_id, account_id, before_bal, after_bal, time.time())
        )
        self.conn.commit()
    
    def rollback_wal(self, txn_id: str) -> bool:
        cursor = self.conn.cursor()
        cursor.execute(
            'SELECT account_id, before_balance FROM wal WHERE txn_id = ? ORDER BY id DESC',
            (txn_id,)
        )
        entries = cursor.fetchall()
        for account_id, before_balance in entries:
            cursor.execute(
                'UPDATE balances SET balance = ? WHERE account_id = ?',
                (before_balance, account_id)
            )
        
        cursor.execute('DELETE FROM wal WHERE txn_id = ?', (txn_id,))
        self.conn.commit()
        return len(entries) > 0
    
    def clear_wal(self, txn_id: str):
        cursor = self.conn.cursor()
        cursor.execute('DELETE FROM wal WHERE txn_id = ?', (txn_id,))
        self.conn.commit()
    
    def get_all_balances(self) -> Dict[int, int]:
        cursor = self.conn.cursor()
        cursor.execute('SELECT account_id, balance FROM balances ORDER BY account_id')
        return {row[0]: row[1] for row in cursor.fetchall()}
    
    def get_modified_items(self) -> Dict[int, int]:
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT b.account_id, b.balance 
            FROM balances b 
            INNER JOIN modified_items m ON b.account_id = m.account_id
            ORDER BY b.account_id
        ''')
        return {row[0]: row[1] for row in cursor.fetchall()}
    
    def mark_modified(self, account_id: int):
        cursor = self.conn.cursor()
        cursor.execute(
            'INSERT OR IGNORE INTO modified_items (account_id) VALUES (?)',
            (account_id,)
        )
        self.conn.commit()
    
    def reset(self):
        cursor = self.conn.cursor()
        cursor.execute('DELETE FROM balances')
        cursor.execute('DELETE FROM wal')
        cursor.execute('DELETE FROM modified_items')
        self.conn.commit()
        self.init_balances()
    
    def close(self):
        self.conn.close()



class LockManager:
    
    def __init__(self):
        self._locks: Dict[int, str] = {}  # account_id -> txn_id
        self._lock = asyncio.Lock()
    
    async def try_acquire(self, account_id: int, txn_id: str) -> bool:
        async with self._lock:
            if account_id in self._locks:
                # Already locked by another transaction
                return self._locks[account_id] == txn_id  # OK if same txn
            self._locks[account_id] = txn_id
            return True
    
    async def try_acquire_multiple(self, account_ids: List[int], txn_id: str) -> bool:
        async with self._lock:
            # Check if any are locked
            for aid in account_ids:
                if aid in self._locks and self._locks[aid] != txn_id:
                    return False
            # Acquire all
            for aid in account_ids:
                self._locks[aid] = txn_id
            return True
    
    async def release(self, account_id: int, txn_id: str):
        async with self._lock:
            if self._locks.get(account_id) == txn_id:
                del self._locks[account_id]
    
    async def release_multiple(self, account_ids: List[int], txn_id: str):
        async with self._lock:
            for aid in account_ids:
                if self._locks.get(aid) == txn_id:
                    del self._locks[aid]
    
    def is_locked(self, account_id: int) -> bool:
        return account_id in self._locks
    
    def get_lock_holder(self, account_id: int) -> Optional[str]:
        return self._locks.get(account_id)
    
    def clear_all(self):
        self._locks.clear()
    
    def get_all_locks(self) -> Dict[int, str]:
        return self._locks.copy()



@dataclass
class NodeState:
    node_id: str
    cluster_id: str
    cluster_nodes: List[str]
    
   
    log: Dict[int, LogEntry] = field(default_factory=dict)
    next_seq: int = 1
    last_executed_seq: int = 0
    last_reply: Dict[str, Tuple[int, Reply]] = field(default_factory=dict)
    
   
    is_leader: bool = False
    current_ballot: Optional[Ballot] = None
    max_ballot_seen: Optional[Ballot] = None
    known_leader_id: Optional[str] = None
    
   
    election_in_progress: bool = False
    election_ballot: Optional[Ballot] = None
    election_acks: Set[str] = field(default_factory=set)
    election_accept_log: List[Dict] = field(default_factory=list)
    accept_quorum: Dict[int, Set[str]] = field(default_factory=dict)
    
   
    is_failed: bool = False
    election_timer_expired: bool = False
    pending_prepares: List[Dict] = field(default_factory=list)
    last_prepare_time: float = 0.0
    last_view_change_time: float = 0.0
    
    
    new_view_history: List[Dict] = field(default_factory=list)
    
   
    cross_shard_txns: Dict[str, CrossShardTxn] = field(default_factory=dict)
  
    cluster_leaders: Dict[str, str] = field(default_factory=dict)


class NodeConnectionPool:
    
    def __init__(self, logger):
        self._writers: Dict[str, asyncio.StreamWriter] = {}
        self._readers: Dict[str, asyncio.StreamReader] = {}
        self._locks: Dict[str, asyncio.Lock] = {}
        self._logger = logger
        
    def _get_lock(self, key: str) -> asyncio.Lock:
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()
        return self._locks[key]
    
    async def send(self, host: str, port: int, msg: Dict) -> bool:
        key = f"{host}:{port}"
        lock = self._get_lock(key)
        
        async with lock:
            max_retries = 3
            for attempt in range(max_retries):
                try:
                   
                    writer = self._writers.get(key)
                    if writer is None or writer.is_closing():
                     
                        reader, writer = await asyncio.wait_for(
                            asyncio.open_connection(host, port),
                            timeout=2.0
                        )
                        self._writers[key] = writer
                        self._readers[key] = reader
                    
                   
                    writer = self._writers[key]
                    writer.write((json.dumps(msg) + "\n").encode())
                    await writer.drain()
                    return True
                    
                except Exception as e:
                   
                    await self._close_connection(key)
                    if attempt < max_retries - 1:
                        await asyncio.sleep(0.1 * (attempt + 1))
                    else:
                        msg_type = msg.get("type", "unknown")
                        self._logger.warning(f"SEND FAILED to {key} type={msg_type}: {e}")
                        return False
        return False
    
    async def _close_connection(self, key: str):
        writer = self._writers.pop(key, None)
        self._readers.pop(key, None)
        if writer and not writer.is_closing():
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
    
    async def close_all(self):
        for key in list(self._writers.keys()):
            await self._close_connection(key)


class NodeServer:
    
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.cluster_id = get_cluster_for_node(node_id)
        self.cluster_nodes = get_nodes_in_cluster(self.cluster_id)
        self.quorum_size = get_quorum_size(self.cluster_id)
        
        self.state = NodeState(
            node_id=node_id,
            cluster_id=self.cluster_id,
            cluster_nodes=self.cluster_nodes
        )
  
        for cid in CLUSTER_CONFIG:
            self.state.cluster_leaders[cid] = CLUSTER_CONFIG[cid]["initial_leader"]
        
       
        self.db = DatabaseManager(node_id, self.cluster_id)
        self.db.init_balances()
        
     
        self.lock_mgr = LockManager()
        
       
        self.host = NODE_CONFIG[node_id]["host"]
        self.port = NODE_CONFIG[node_id]["port"]
        self.logger = logging.getLogger(node_id)
        
       
        self.conn_pool = NodeConnectionPool(self.logger)
        
       
        self.timer_task = None
        self.heartbeat_task = None
        self.tpc_timeout_tasks: Dict[str, asyncio.Task] = {}
        self.timers_paused = False 
        
    
        if node_id == CLUSTER_CONFIG[self.cluster_id]["initial_leader"]:
            self.state.is_leader = True
            self.state.current_ballot = (1, node_id)
            self.state.max_ballot_seen = (1, node_id)
            self.state.known_leader_id = node_id
            self.state.last_view_change_time = time.monotonic()
    
   
    
    async def start(self):
        server = await asyncio.start_server(self.handle_conn, self.host, self.port)
        self.logger.info(f"Node {self.node_id} ({self.cluster_id}) listening on {self.host}:{self.port}")
        self.reset_timer()
        if self.state.is_leader:
            self.start_heartbeats()
            
            await self.announce_leadership()
        async with server:
            await server.serve_forever()
    
    async def handle_conn(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                try:
                    msg = json.loads(line)
                    await self.process_msg(msg)
                except json.JSONDecodeError:
                    self.logger.warning(f"Invalid JSON received")
        except Exception as e:
            self.logger.debug(f"Connection error: {e}")
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except:
                pass
    

    async def process_msg(self, msg: Dict):
        mtype = msg.get("type")
        
    
        if mtype == MSG_CONTROL:
            await self.handle_control(msg)
            return
        
    
        if self.state.is_failed:
            return
       
        handlers = {
            MSG_REQUEST: self.handle_request,
            MSG_PREPARE: self.handle_prepare,
            MSG_ACK: self.handle_ack,
            MSG_ACCEPT: self.handle_accept,
            MSG_ACCEPTED: self.handle_accepted,
            MSG_COMMIT: self.handle_commit,
            MSG_NEW_VIEW: self.handle_new_view,
            MSG_HEARTBEAT: self.handle_heartbeat,
            MSG_BALANCE_REQUEST: self.handle_balance_request,
            MSG_2PC_PREPARE: self.handle_2pc_prepare,
            MSG_2PC_PREPARED: self.handle_2pc_prepared,
            MSG_2PC_ABORT: self.handle_2pc_abort,
            MSG_2PC_COMMIT: self.handle_2pc_commit,
            MSG_2PC_ACK: self.handle_2pc_ack,
            MSG_LEADER_ANNOUNCE: self.handle_leader_announce,
        }
        
        handler = handlers.get(mtype)
        if handler:
            await handler(msg)
        else:
            self.logger.warning(f"Unknown message type: {mtype}")
    
   
    def reset_timer(self):
        if self.timer_task:
            self.timer_task.cancel()
        
        if self.state.is_leader or self.state.is_failed or self.timers_paused:
            return
        
        async def _timer():
            try:
             
                node_offset = int(self.node_id[1]) * 0.15
                jitter = random.uniform(0, 1.5) + node_offset
                timeout_sec = (LEADER_TIMEOUT_MS / 1000.0) + jitter
                await asyncio.sleep(timeout_sec)
                
                if not self.state.is_failed and not self.state.is_leader:
                    self.state.election_timer_expired = True
                    self.logger.info(f"Timer expired, known_leader={self.state.known_leader_id}")
                    
                    if self.state.pending_prepares:
                        
                        best = max(self.state.pending_prepares, key=lambda m: tuple(m["ballot"]))
                        self.state.pending_prepares.clear()
                        await self.accept_prepare_msg(best)
                    else:
                        if self.state.election_in_progress:
                            self.state.election_in_progress = False
                        await self.start_election()
            except asyncio.CancelledError:
                pass
        
        self.timer_task = asyncio.create_task(_timer())
    
    def start_heartbeats(self):
        if self.heartbeat_task and not self.heartbeat_task.done():
            self.heartbeat_task.cancel()
        
        async def _hb():
            while self.state.is_leader and not self.state.is_failed:
                try:
                    await asyncio.sleep(HEARTBEAT_INTERVAL_MS / 1000.0)
                    msg = base_msg(MSG_HEARTBEAT, self.node_id, "broadcast")
                    msg["ballot"] = list(self.state.current_ballot)
                    await self.broadcast_cluster(msg)
                except asyncio.CancelledError:
                    break
        
        self.heartbeat_task = asyncio.create_task(_hb())
    
   
    
    def check_step_down(self, ballot: Tuple):
        ballot = tuple(ballot)
        if self.state.current_ballot and ballot > self.state.current_ballot:
            if self.state.is_leader:
                self.logger.info(f"Stepping down, saw higher ballot {ballot}")
                self.state.is_leader = False
                self.state.election_in_progress = False
                if self.heartbeat_task:
                    self.heartbeat_task.cancel()
                self.reset_timer()
            if not self.state.max_ballot_seen or ballot > self.state.max_ballot_seen:
                self.state.max_ballot_seen = ballot
    
   
    
    async def start_election(self):
        if self.state.election_in_progress:
            return
        
       
        time_since_prepare = (time.monotonic() - self.state.last_prepare_time) * 1000
        if time_since_prepare < PREPARE_SUPPRESSION_MS:
            self.logger.info(f"Suppressing election, recent PREPARE")
            self.reset_timer()
            return
        
        time_since_view = (time.monotonic() - self.state.last_view_change_time) * 1000
        if time_since_view < 1500:
            self.reset_timer()
            return
        
        prev = self.state.max_ballot_seen or (0, "")
        new_ballot = (prev[0] + 1, self.node_id)
        
        self.state.current_ballot = new_ballot
        self.state.max_ballot_seen = new_ballot
        self.state.election_in_progress = True
        self.state.election_ballot = new_ballot
        self.state.election_acks = {self.node_id}
        self.state.election_accept_log = self.build_accept_log()
        
        self.logger.info(f"Starting election with ballot {new_ballot}")
        
        msg = base_msg(MSG_PREPARE, self.node_id, "broadcast")
        msg["ballot"] = list(new_ballot)
        await self.broadcast_cluster(msg)
        self.reset_timer()
    
    async def handle_prepare(self, msg: Dict):
        ballot = tuple(msg["ballot"])
        src = msg["src"]
        
        self.state.last_prepare_time = time.monotonic()
        
       
        if self.state.election_in_progress and ballot > self.state.election_ballot:
            self.state.election_in_progress = False
        
        if not self.state.election_timer_expired:
            self.state.pending_prepares.append(msg)
            return
        
        await self.accept_prepare_msg(msg)
    
    async def accept_prepare_msg(self, msg: Dict):
        ballot = tuple(msg["ballot"])
        src = msg["src"]
        
        if self.state.max_ballot_seen and ballot < self.state.max_ballot_seen:
            return
        if self.state.max_ballot_seen and ballot == self.state.max_ballot_seen and src != self.node_id:
            return
        
        self.state.max_ballot_seen = ballot
        self.state.known_leader_id = src
        
        accept_log = self.build_accept_log()
        ack = base_msg(MSG_ACK, self.node_id, src)
        ack["ballot"] = list(ballot)
        ack["accept_log"] = accept_log
        await self.send_to_node(src, ack)
    
    async def handle_ack(self, msg: Dict):
        ballot = tuple(msg["ballot"])
        src = msg["src"]
        
        if not self.state.election_in_progress:
            return
        if ballot != self.state.election_ballot:
            return
        if src in self.state.election_acks:
            return
        
        self.state.election_acks.add(src)
        self.state.election_accept_log.extend(msg.get("accept_log", []))
        
        if len(self.state.election_acks) >= self.quorum_size:
            self.logger.info(f"Election won with ballot {ballot}")
            self.state.is_leader = True
            self.state.election_in_progress = False
            self.state.accept_quorum.clear()
            self.state.pending_prepares.clear()
            
            nv_log = self.build_new_view_log()
            
           
            new_view_record = {
                "ballot": list(ballot),
                "accept_log": nv_log,
                "timestamp": datetime.now().strftime("%H:%M:%S.%f")[:-3],
                "acks_from": list(self.state.election_acks),
                "max_seq": len(nv_log) if nv_log else 0
            }
            self.state.new_view_history.append(new_view_record)
            
            nv = base_msg(MSG_NEW_VIEW, self.node_id, "broadcast")
            nv["ballot"] = list(ballot)
            nv["accept_log"] = nv_log
            
            self.start_heartbeats()
            await self.broadcast_cluster(nv)
            
            
            await self.announce_leadership()
    
    async def handle_new_view(self, msg: Dict):
        ballot = tuple(msg["ballot"])
        src = msg["src"]
        
        self.reset_timer()
        self.check_step_down(ballot)
        
        if self.state.max_ballot_seen and ballot < self.state.max_ballot_seen:
            return
        
        self.state.max_ballot_seen = ballot
        self.state.current_ballot = ballot
        self.state.known_leader_id = src
        self.state.is_leader = (self.node_id == src)
        self.state.election_in_progress = False
        self.state.last_view_change_time = time.monotonic()
        self.state.pending_prepares.clear()
        
       
        self.state.cluster_leaders[self.cluster_id] = src
        
        if not self.state.is_leader:
            new_view_record = {
                "ballot": list(ballot),
                "accept_log": msg["accept_log"],
                "timestamp": datetime.now().strftime("%H:%M:%S.%f")[:-3],
                "received_from": src,
                "max_seq": len(msg["accept_log"])
            }
            self.state.new_view_history.append(new_view_record)
        
        if self.state.is_leader:
            self.start_heartbeats()
        
        
        for item in msg["accept_log"]:
            seq = item["seq"]
            val = "no-op" if item["kind"] == "no-op" else Request.from_dict(item["request"])
            
            if seq in self.state.log:
                entry = self.state.log[seq]
                entry.ballot = ballot
                if not entry.committed:
                    entry.value = val
                entry.accepted = True
            else:
                self.state.log[seq] = LogEntry(seq, ballot, val, accepted=True)
            
            if self.state.is_leader:
                self.state.accept_quorum.setdefault(seq, set()).add(self.node_id)
            else:
                acc = base_msg(MSG_ACCEPTED, self.node_id, src)
                acc["ballot"] = list(ballot)
                acc["seq"] = seq
                acc["request"] = item.get("request")
                await self.send_to_node(src, acc)
        
        if self.state.log:
            self.state.next_seq = max(self.state.log.keys()) + 1
    
    async def handle_heartbeat(self, msg: Dict):
        ballot = tuple(msg["ballot"])
        src = msg["src"]
        
        self.check_step_down(ballot)
        if self.state.max_ballot_seen and ballot < self.state.max_ballot_seen:
            return
        
        self.state.max_ballot_seen = ballot
        self.state.known_leader_id = src
        self.state.cluster_leaders[self.cluster_id] = src
        self.reset_timer()
    
    
    
    async def handle_request(self, msg: Dict):
        req = Request.from_dict(msg)
        txn = req.txn
        
        self.logger.info(f"REQUEST received: {txn.sender}->{txn.receiver} ${txn.amount} (is_leader={self.state.is_leader})")
        
       
        if is_cross_shard(txn):
            txn.txn_type = TxnType.CROSS_SHARD
        else:
            txn.txn_type = TxnType.INTRA_SHARD
        
       
        if req.client_id in self.state.last_reply:
            last_ts, last_reply = self.state.last_reply[req.client_id]
            if req.timestamp <= last_ts:
                if req.timestamp == last_ts:
                    self.logger.info(f"Duplicate request, resending cached reply")
                    await self.send_reply(req.client_id, make_reply(self.node_id, req.client_id, last_reply))
                return
        
        
        if not self.state.is_leader:
            if self.state.known_leader_id and self.state.known_leader_id != self.node_id:
                self.logger.info(f"Not leader, forwarding to {self.state.known_leader_id}")
                fwd = msg.copy()
                fwd["dst"] = self.state.known_leader_id
                await self.send_to_node(self.state.known_leader_id, fwd)
            return
        
      
        for seq, entry in self.state.log.items():
            if isinstance(entry.value, Request):
                if (entry.value.client_id == req.client_id and 
                    entry.value.timestamp == req.timestamp):
                    
                    self.logger.debug(f"Request {req.request_id} already pending at seq {seq}, ignoring retry")
                    return
        

        if txn.txn_type == TxnType.CROSS_SHARD:
            await self.process_cross_shard_as_coordinator(req)
        else:
            await self.process_intra_shard(req)
    
    async def process_intra_shard(self, req: Request):
        txn = req.txn
        txn_id = f"intra:{req.request_id}"
        
        self.logger.info(f"Processing INTRA-SHARD: {txn.sender}->{txn.receiver} ${txn.amount}")
        
    
        accounts = [txn.sender, txn.receiver]
        if not await self.lock_mgr.try_acquire_multiple(accounts, txn_id):
            self.logger.info(f"LOCKED: Skipping {req.request_id}, accounts {accounts} locked")
            reply = Reply(self.state.current_ballot, req.client_id, req.timestamp, "locked")
            await self.send_reply(req.client_id, make_reply(self.node_id, req.client_id, reply))
            return
        
        self.logger.info(f"LOCKS acquired for {accounts}")
        
      
        seq = self.state.next_seq
        while seq in self.state.log:
            seq += 1
        self.state.next_seq = seq + 1
        
        entry = LogEntry(seq, self.state.current_ballot, req, accepted=True, accept_flag=AcceptFlag.NORMAL)
        self.state.log[seq] = entry
        self.state.accept_quorum[seq] = {self.node_id}
        
        self.logger.info(f"ACCEPT broadcast for seq={seq}: {txn.sender}->{txn.receiver}")
        
       
        acc = base_msg(MSG_ACCEPT, self.node_id, "broadcast")
        acc["ballot"] = list(self.state.current_ballot)
        acc["seq"] = seq
        acc["request"] = req.to_dict()
        acc["flag"] = AcceptFlag.NORMAL.value
        
        await self.broadcast_cluster(acc)
    
    async def handle_accept(self, msg: Dict):
        self.reset_timer()
        ballot = tuple(msg["ballot"])
        self.check_step_down(ballot)
        
        if self.state.max_ballot_seen and ballot < self.state.max_ballot_seen:
            return
        
        self.state.max_ballot_seen = ballot
        self.state.known_leader_id = msg["src"]
        
        seq = int(msg["seq"])
        flag = AcceptFlag(msg.get("flag", AcceptFlag.NORMAL.value))
        txn_id = msg.get("txn_id") 
        req_data = msg.get("request")
        
        if req_data:
            req = Request.from_dict(req_data)
        else:
            req = "no-op"
        
        if seq in self.state.log:
            entry = self.state.log[seq]
            entry.ballot = ballot
            if not entry.committed:
                entry.value = req
            entry.accepted = True
            entry.accept_flag = flag
            entry.txn_id = txn_id 
        else:
            self.state.log[seq] = LogEntry(seq, ballot, req, accepted=True, accept_flag=flag, txn_id=txn_id)
        
        if not self.state.is_leader:
            reply = base_msg(MSG_ACCEPTED, self.node_id, msg["src"])
            reply["ballot"] = list(ballot)
            reply["seq"] = seq
            reply["request"] = req_data
            reply["flag"] = flag.value
            await self.send_to_node(msg["src"], reply)
    
    async def handle_accepted(self, msg: Dict):
        if not self.state.is_leader:
            return
        
        seq = int(msg["seq"])
        src = msg["src"]
        if seq not in self.state.log:
            return
        
        self.state.accept_quorum.setdefault(seq, set()).add(src)
        quorum_count = len(self.state.accept_quorum[seq])
        
        self.logger.info(f"ACCEPTED from {src} for seq={seq}, quorum={quorum_count}/{self.quorum_size}")
        
        if quorum_count >= self.quorum_size:
            entry = self.state.log[seq]
            
            if not entry.committed or entry.ballot != self.state.current_ballot:
                entry.committed = True
                entry.ballot = self.state.current_ballot
                
                self.logger.info(f"QUORUM REACHED for seq={seq}, broadcasting COMMIT")
                
              
                commit = base_msg(MSG_COMMIT, self.node_id, "broadcast")
                commit["ballot"] = list(self.state.current_ballot)
                commit["seq"] = seq
                commit["flag"] = entry.accept_flag.value
                if isinstance(entry.value, Request):
                    commit["request"] = entry.value.to_dict()
                
                await self.broadcast_cluster(commit)
                await self.execute_log()
    
    async def handle_commit(self, msg: Dict):
        self.reset_timer()
        ballot = tuple(msg["ballot"])
        self.check_step_down(ballot)
        seq = int(msg["seq"])
        
        self.state.max_ballot_seen = ballot
        self.state.known_leader_id = msg["src"]
        
        if seq in self.state.log:
            entry = self.state.log[seq]
            entry.committed = True
            entry.ballot = ballot
            if entry.value is None and msg.get("request"):
                entry.value = Request.from_dict(msg["request"])
        else:
            req_data = msg.get("request")
            val = Request.from_dict(req_data) if req_data else "no-op"
            flag = AcceptFlag(msg.get("flag", AcceptFlag.NORMAL.value))
            self.state.log[seq] = LogEntry(seq, ballot, val, accepted=True, committed=True, accept_flag=flag)
        
        await self.execute_log()
    
    async def execute_log(self):
        while True:
            nxt = self.state.last_executed_seq + 1
            if nxt not in self.state.log:
                break
            
            entry = self.state.log[nxt]
            if not entry.committed or entry.executed:
                if entry.executed:
                    self.state.last_executed_seq = nxt
                    continue
                break
            
            entry.executed = True
            self.state.last_executed_seq = nxt
            
            if isinstance(entry.value, Request):
                req = entry.value
                txn = req.txn
                txn_id = f"intra:{req.request_id}"
                
               
                if entry.accept_flag == AcceptFlag.NORMAL:
                   
                    self.logger.info(f"EXECUTING seq={nxt}: {txn.sender}->{txn.receiver} ${txn.amount}")
                    success, reason = self.db.transfer(txn.sender, txn.receiver, txn.amount)
                    result = "success" if success else "failure"
                    
                    if success:
                        
                        new_sender_bal = self.db.get_balance(txn.sender)
                        new_recv_bal = self.db.get_balance(txn.receiver)
                        self.logger.info(f"EXECUTED: {txn.sender}={new_sender_bal}, {txn.receiver}={new_recv_bal}")
                    else:
                        self.logger.warning(f"EXECUTION FAILED: {reason}")
                    
                   
                    await self.lock_mgr.release_multiple([txn.sender, txn.receiver], txn_id)
                    
                  
                    if self.state.is_leader:
                        b = entry.ballot or (0, "")
                        reply = Reply(b, req.client_id, req.timestamp, result)
                        self.state.last_reply[req.client_id] = (req.timestamp, reply)
                        self.logger.info(f"REPLY sent to {req.client_id}: {result}")
                        await self.send_reply(req.client_id, make_reply(self.node_id, req.client_id, reply))
                
                elif entry.accept_flag == AcceptFlag.PREPARE:
                    
                    await self.execute_2pc_prepare(entry)
                
                elif entry.accept_flag == AcceptFlag.COMMIT:
              
                    await self.execute_2pc_commit(entry)
                
                elif entry.accept_flag == AcceptFlag.ABORT:
                   
                    await self.execute_2pc_abort(entry)
            else:
                self.logger.debug(f"Executed seq={nxt}: no-op")
    
  
    async def handle_balance_request(self, msg: Dict):
        account_id = int(msg["account_id"])
        client_id = msg.get("client_id", "client")
        timestamp = msg.get("timestamp", 0)
        
        self.logger.info(f"BALANCE REQUEST for account {account_id} (is_leader={self.state.is_leader})")
        
        
        if get_cluster_for_item(account_id) != self.cluster_id:
            self.logger.warning(f"Balance request for {account_id} but we're {self.cluster_id}")
            return
        
        
        if not self.state.is_leader:
            
            if self.state.known_leader_id and self.state.known_leader_id != self.node_id:
                self.logger.info(f"Forwarding balance request to leader {self.state.known_leader_id}")
                msg["dst"] = self.state.known_leader_id
                await self.send_to_node(self.state.known_leader_id, msg)
                return
            
        
        balance = self.db.get_balance(account_id)
        self.logger.info(f"BALANCE REPLY: account {account_id} = {balance}")
        
        if balance is not None:
            reply = make_balance_reply(self.node_id, client_id, client_id, timestamp, balance)
            await self.send_reply(client_id, reply)
        else:
            reply = make_balance_reply(self.node_id, client_id, client_id, timestamp, 0, "not_found")
            await self.send_reply(client_id, reply)
    
   
    
    async def process_cross_shard_as_coordinator(self, req: Request):
        txn = req.txn
        txn_id = generate_txn_id(req)
        
        sender_cluster = get_cluster_for_item(txn.sender)
        receiver_cluster = get_cluster_for_item(txn.receiver)
        
        self.logger.info(f"Cross-shard txn {txn_id}: {txn.sender}->{txn.receiver} (coord={sender_cluster}, part={receiver_cluster})")
        
       
        balance = self.db.get_balance(txn.sender)
        if balance is None or balance < txn.amount:
            self.logger.info(f"Cross-shard {txn_id}: insufficient balance ({balance} < {txn.amount})")
            reply = Reply(self.state.current_ballot, req.client_id, req.timestamp, "failure")
            await self.send_reply(req.client_id, make_reply(self.node_id, req.client_id, reply))
            return
        
        if not await self.lock_mgr.try_acquire(txn.sender, txn_id):
            self.logger.info(f"Cross-shard {txn_id}: sender locked")
            reply = Reply(self.state.current_ballot, req.client_id, req.timestamp, "locked")
            await self.send_reply(req.client_id, make_reply(self.node_id, req.client_id, reply))
            return
        
        
        cs_txn = CrossShardTxn(
            txn_id=txn_id,
            request=req,
            state=TxnState.PREPARING,
            coordinator_cluster=sender_cluster,
            participant_cluster=receiver_cluster,
            is_coordinator=True,
            created_at=time.time()
        )
        self.state.cross_shard_txns[txn_id] = cs_txn
        
       
        seq = self.state.next_seq
        while seq in self.state.log:
            seq += 1
        self.state.next_seq = seq + 1
        cs_txn.prepare_seq = seq
        
        entry = LogEntry(seq, self.state.current_ballot, req, accepted=True, 
                        accept_flag=AcceptFlag.PREPARE, txn_id=txn_id)
        self.state.log[seq] = entry
        self.state.accept_quorum[seq] = {self.node_id}
        
        acc = base_msg(MSG_ACCEPT, self.node_id, "broadcast")
        acc["ballot"] = list(self.state.current_ballot)
        acc["seq"] = seq
        acc["request"] = req.to_dict()
        acc["flag"] = AcceptFlag.PREPARE.value
        acc["txn_id"] = txn_id
        
        await self.broadcast_cluster(acc)
       
        participant_nodes = get_nodes_in_cluster(receiver_cluster)
        prep_msg = base_msg(MSG_2PC_PREPARE, self.node_id, "participant")
        prep_msg["txn_id"] = txn_id
        prep_msg["request"] = req.to_dict()
        prep_msg["coordinator_cluster"] = sender_cluster
        prep_msg["participant_cluster"] = receiver_cluster
        prep_msg["coordinator_leader"] = self.node_id  
        
        self.logger.info(f"Broadcasting 2PC_PREPARE to all nodes in {receiver_cluster}: {participant_nodes}")
        for node in participant_nodes:
            await self.send_to_node(node, prep_msg)
        
       
        self.start_2pc_timeout(txn_id)
    
    async def handle_2pc_prepare(self, msg: Dict):
        txn_id = msg["txn_id"]
        req = Request.from_dict(msg["request"])
        txn = req.txn
        coordinator_cluster = msg["coordinator_cluster"]
        coordinator_leader = msg.get("coordinator_leader", msg["src"])  
        
        
        if txn_id in self.state.cross_shard_txns:
            cs_txn = self.state.cross_shard_txns[txn_id]
            
            if cs_txn.state in [TxnState.PREPARING, TxnState.PREPARED, TxnState.COMMITTING, TxnState.COMMITTED]:
                
                if self.state.is_leader and cs_txn.state in [TxnState.PREPARED, TxnState.COMMITTING, TxnState.COMMITTED]:
                    self.logger.info(f"Resending PREPARED for {txn_id}")
                    prep_msg = base_msg(MSG_2PC_PREPARED, self.node_id, coordinator_leader)
                    prep_msg["txn_id"] = txn_id
                    prep_msg["request"] = req.to_dict()
                    await self.send_to_node(coordinator_leader, prep_msg)
                return
            elif cs_txn.state in [TxnState.ABORTING, TxnState.ABORTED]:
               
                if self.state.is_leader:
                    abort_msg = base_msg(MSG_2PC_ABORT, self.node_id, coordinator_leader)
                    abort_msg["txn_id"] = txn_id
                    abort_msg["request"] = req.to_dict()
                    await self.send_to_node(coordinator_leader, abort_msg)
                return
        
      
        if not self.state.is_leader:
           
            if self.state.known_leader_id and self.state.known_leader_id != self.node_id:
                await self.send_to_node(self.state.known_leader_id, msg)
            return
        
        receiver = txn.receiver
        
        self.logger.info(f"2PC PREPARE received: {txn_id} (receiver={receiver})")
        
       
        if not await self.lock_mgr.try_acquire(receiver, txn_id):
            self.logger.info(f"2PC {txn_id}: receiver {receiver} locked, aborting")
           
            await self.run_abort_consensus(txn_id, req, is_coordinator=False)
            
            
            self.state.cluster_leaders[coordinator_cluster] = coordinator_leader
            
            abort_msg = base_msg(MSG_2PC_ABORT, self.node_id, coordinator_leader)
            abort_msg["txn_id"] = txn_id
            abort_msg["request"] = req.to_dict()
            await self.send_to_node(coordinator_leader, abort_msg)
            return
        
       
        cs_txn = CrossShardTxn(
            txn_id=txn_id,
            request=req,
            state=TxnState.PREPARING,
            coordinator_cluster=coordinator_cluster,
            participant_cluster=self.cluster_id,
            is_coordinator=False,
            created_at=time.time()
        )
        self.state.cross_shard_txns[txn_id] = cs_txn
        
       
        self.state.cluster_leaders[coordinator_cluster] = coordinator_leader
        
       
        seq = self.state.next_seq
        while seq in self.state.log:
            seq += 1
        self.state.next_seq = seq + 1
        cs_txn.prepare_seq = seq
        
        entry = LogEntry(seq, self.state.current_ballot, req, accepted=True,
                        accept_flag=AcceptFlag.PREPARE, txn_id=txn_id)
        self.state.log[seq] = entry
        self.state.accept_quorum[seq] = {self.node_id}
        
        acc = base_msg(MSG_ACCEPT, self.node_id, "broadcast")
        acc["ballot"] = list(self.state.current_ballot)
        acc["seq"] = seq
        acc["request"] = req.to_dict()
        acc["flag"] = AcceptFlag.PREPARE.value
        acc["txn_id"] = txn_id
        
        await self.broadcast_cluster(acc)
        
       
    
    async def execute_2pc_prepare(self, entry: LogEntry):
        if not isinstance(entry.value, Request):
            return
        
        txn_id = entry.txn_id
        req = entry.value
        txn = req.txn
        
        cs_txn = self.state.cross_shard_txns.get(txn_id)
        
        
        sender_cluster = get_cluster_for_item(txn.sender)
        receiver_cluster = get_cluster_for_item(txn.receiver)
        is_coordinator = (sender_cluster == self.cluster_id)
        is_participant = (receiver_cluster == self.cluster_id)
        
        if is_coordinator:
            
            before_bal = self.db.get_balance(txn.sender)
            if before_bal is not None and before_bal >= txn.amount:
                success, _ = self.db.debit_only(txn.sender, txn.amount)
                if success:
                    self.db.write_wal(txn_id, txn.sender, before_bal, before_bal - txn.amount)
            
            if cs_txn:
                cs_txn.state = TxnState.PREPARED
        
        elif is_participant:
          
            if cs_txn:
                cs_txn.state = TxnState.PREPARED
                cs_txn.prepared_at = time.time()
            
            
            if self.state.is_leader and cs_txn:
                coord_leader = self.state.cluster_leaders.get(cs_txn.coordinator_cluster)
                if coord_leader:
                    prep_msg = base_msg(MSG_2PC_PREPARED, self.node_id, coord_leader)
                    prep_msg["txn_id"] = txn_id
                    prep_msg["request"] = req.to_dict()
                    await self.send_to_node(coord_leader, prep_msg)
    
    async def handle_2pc_prepared(self, msg: Dict):
        if not self.state.is_leader:
            return
        
        txn_id = msg["txn_id"]
        cs_txn = self.state.cross_shard_txns.get(txn_id)
        
        if not cs_txn or not cs_txn.is_coordinator:
            return
        
        if cs_txn.prepared_received:
            return
        
        self.logger.info(f"2PC PREPARED received for {txn_id}")
        cs_txn.prepared_received = True
        cs_txn.prepared_at = time.time()
        
       
        self.cancel_2pc_timeout(txn_id)
       
        await self.run_commit_phase(txn_id)
    
    async def run_commit_phase(self, txn_id: str):
        cs_txn = self.state.cross_shard_txns.get(txn_id)
        if not cs_txn:
            return
        
        cs_txn.state = TxnState.COMMITTING
        req = cs_txn.request
        
       
        seq = self.state.next_seq
        while seq in self.state.log:
            seq += 1
        self.state.next_seq = seq + 1
        cs_txn.final_seq = seq
        
        entry = LogEntry(seq, self.state.current_ballot, req, accepted=True,
                        accept_flag=AcceptFlag.COMMIT, txn_id=txn_id)
        self.state.log[seq] = entry
        self.state.accept_quorum[seq] = {self.node_id}
        
        acc = base_msg(MSG_ACCEPT, self.node_id, "broadcast")
        acc["ballot"] = list(self.state.current_ballot)
        acc["seq"] = seq
        acc["request"] = req.to_dict()
        acc["flag"] = AcceptFlag.COMMIT.value
        acc["txn_id"] = txn_id
        
        await self.broadcast_cluster(acc)
        
        
        participant_nodes = get_nodes_in_cluster(cs_txn.participant_cluster)
        commit_msg = base_msg(MSG_2PC_COMMIT, self.node_id, "participant")
        commit_msg["txn_id"] = txn_id
        commit_msg["request"] = req.to_dict()
        commit_msg["coordinator_leader"] = self.node_id 
        
        self.logger.info(f"Broadcasting 2PC_COMMIT to all nodes in {cs_txn.participant_cluster}")
        for node in participant_nodes:
            await self.send_to_node(node, commit_msg)
       
        self.start_2pc_timeout(txn_id, phase="commit")
    
    async def handle_2pc_commit(self, msg: Dict):
        txn_id = msg["txn_id"]
        req = Request.from_dict(msg["request"])
        coordinator_leader = msg.get("coordinator_leader", msg["src"])
        
        cs_txn = self.state.cross_shard_txns.get(txn_id)
        
       
        if cs_txn and cs_txn.state in [TxnState.COMMITTING, TxnState.COMMITTED]:
           
            if self.state.is_leader and cs_txn.state == TxnState.COMMITTED:
                ack_msg = base_msg(MSG_2PC_ACK, self.node_id, coordinator_leader)
                ack_msg["txn_id"] = txn_id
                await self.send_to_node(coordinator_leader, ack_msg)
            return
        
        if not self.state.is_leader:
            if self.state.known_leader_id and self.state.known_leader_id != self.node_id:
                await self.send_to_node(self.state.known_leader_id, msg)
            return
        
        self.logger.info(f"2PC COMMIT received for {txn_id}")
        
        if not cs_txn:
           
            cs_txn = CrossShardTxn(
                txn_id=txn_id,
                request=req,
                state=TxnState.COMMITTING,
                is_coordinator=False,
                created_at=time.time()
            )
            self.state.cross_shard_txns[txn_id] = cs_txn
        
        
        if cs_txn.coordinator_cluster:
            self.state.cluster_leaders[cs_txn.coordinator_cluster] = coordinator_leader
        
        cs_txn.state = TxnState.COMMITTING
        
        
        seq = self.state.next_seq
        while seq in self.state.log:
            seq += 1
        self.state.next_seq = seq + 1
        cs_txn.final_seq = seq
        
        entry = LogEntry(seq, self.state.current_ballot, req, accepted=True,
                        accept_flag=AcceptFlag.COMMIT, txn_id=txn_id)
        self.state.log[seq] = entry
        self.state.accept_quorum[seq] = {self.node_id}
        
        acc = base_msg(MSG_ACCEPT, self.node_id, "broadcast")
        acc["ballot"] = list(self.state.current_ballot)
        acc["seq"] = seq
        acc["request"] = req.to_dict()
        acc["flag"] = AcceptFlag.COMMIT.value
        acc["txn_id"] = txn_id
        
        await self.broadcast_cluster(acc)
    
    async def execute_2pc_commit(self, entry: LogEntry):
        if not isinstance(entry.value, Request):
            return
        
        txn_id = entry.txn_id
        req = entry.value
        txn = req.txn
        
        cs_txn = self.state.cross_shard_txns.get(txn_id)
        
        
        sender_cluster = get_cluster_for_item(txn.sender)
        receiver_cluster = get_cluster_for_item(txn.receiver)
        is_coordinator = (sender_cluster == self.cluster_id)
        is_participant = (receiver_cluster == self.cluster_id)
        
        if is_coordinator:
            
            self.db.clear_wal(txn_id)
            await self.lock_mgr.release(txn.sender, txn_id)
            
            if cs_txn:
                cs_txn.state = TxnState.COMMITTED
                cs_txn.completed_at = time.time()
            
           
            if self.state.is_leader:
                b = entry.ballot or (0, "")
                reply = Reply(b, req.client_id, req.timestamp, "success")
                self.state.last_reply[req.client_id] = (req.timestamp, reply)
                await self.send_reply(req.client_id, make_reply(self.node_id, req.client_id, reply))
        
        elif is_participant:
           
            self.db.credit_only(txn.receiver, txn.amount)
            await self.lock_mgr.release(txn.receiver, txn_id)
            
            if cs_txn:
                cs_txn.state = TxnState.COMMITTED
                cs_txn.completed_at = time.time()
           
            if self.state.is_leader and cs_txn:
                coord_leader = self.state.cluster_leaders.get(cs_txn.coordinator_cluster)
                if coord_leader:
                    ack_msg = base_msg(MSG_2PC_ACK, self.node_id, coord_leader)
                    ack_msg["txn_id"] = txn_id
                    await self.send_to_node(coord_leader, ack_msg)
    
    async def handle_2pc_ack(self, msg: Dict):
        txn_id = msg["txn_id"]
        cs_txn = self.state.cross_shard_txns.get(txn_id)
        
        if cs_txn:
            cs_txn.ack_received = True
            self.cancel_2pc_timeout(txn_id)
    
    async def handle_2pc_abort(self, msg: Dict):
        txn_id = msg["txn_id"]
        req = Request.from_dict(msg["request"])
        
        cs_txn = self.state.cross_shard_txns.get(txn_id)
        
        self.logger.info(f"2PC ABORT received for {txn_id}, is_coordinator={cs_txn.is_coordinator if cs_txn else 'unknown'}")
        
        if cs_txn and cs_txn.is_coordinator:
           
            if not self.state.is_leader:
                if self.state.known_leader_id and self.state.known_leader_id != self.node_id:
                    await self.send_to_node(self.state.known_leader_id, msg)
                return
            
           
            self.cancel_2pc_timeout(txn_id)
            
           
            if cs_txn.state not in [TxnState.ABORTING, TxnState.ABORTED]:
                await self.run_abort_consensus(txn_id, req, is_coordinator=True)
        
        elif cs_txn and not cs_txn.is_coordinator:
           
            if not self.state.is_leader:
                if self.state.known_leader_id and self.state.known_leader_id != self.node_id:
                    await self.send_to_node(self.state.known_leader_id, msg)
                return
            
            
            if cs_txn.state not in [TxnState.ABORTING, TxnState.ABORTED]:
                await self.run_abort_consensus(txn_id, req, is_coordinator=False)
        
        else:
            
            if not self.state.is_leader:
                if self.state.known_leader_id and self.state.known_leader_id != self.node_id:
                    await self.send_to_node(self.state.known_leader_id, msg)
                return
           
            await self.run_abort_consensus(txn_id, req, is_coordinator=False)
    
    async def run_abort_consensus(self, txn_id: str, req: Request, is_coordinator: bool = True):
        cs_txn = self.state.cross_shard_txns.get(txn_id)
        if cs_txn:
            cs_txn.state = TxnState.ABORTING
            cs_txn.is_coordinator = is_coordinator
        else:
            
            cs_txn = CrossShardTxn(
                txn_id=txn_id,
                request=req,
                state=TxnState.ABORTING,
                is_coordinator=is_coordinator,
                created_at=time.time()
            )
            self.state.cross_shard_txns[txn_id] = cs_txn
        
        seq = self.state.next_seq
        while seq in self.state.log:
            seq += 1
        self.state.next_seq = seq + 1
        
        cs_txn.final_seq = seq
        
        entry = LogEntry(seq, self.state.current_ballot, req, accepted=True,
                        accept_flag=AcceptFlag.ABORT, txn_id=txn_id)
        self.state.log[seq] = entry
        self.state.accept_quorum[seq] = {self.node_id}
        
        acc = base_msg(MSG_ACCEPT, self.node_id, "broadcast")
        acc["ballot"] = list(self.state.current_ballot)
        acc["seq"] = seq
        acc["request"] = req.to_dict()
        acc["flag"] = AcceptFlag.ABORT.value
        acc["txn_id"] = txn_id
        
        await self.broadcast_cluster(acc)
    
    async def execute_2pc_abort(self, entry: LogEntry):
        if not isinstance(entry.value, Request):
            return
        
        txn_id = entry.txn_id
        req = entry.value
        txn = req.txn
        
        cs_txn = self.state.cross_shard_txns.get(txn_id)
        
       
        sender_cluster = get_cluster_for_item(txn.sender)
        receiver_cluster = get_cluster_for_item(txn.receiver)
        is_coordinator = (sender_cluster == self.cluster_id)
        is_participant = (receiver_cluster == self.cluster_id)
        
        if is_coordinator:
            
            self.db.rollback_wal(txn_id)
            await self.lock_mgr.release(txn.sender, txn_id)
            
            if cs_txn:
                cs_txn.state = TxnState.ABORTED
                cs_txn.completed_at = time.time()
            
            if self.state.is_leader:
                
                b = entry.ballot or (0, "")
                reply = Reply(b, req.client_id, req.timestamp, "aborted")
                self.state.last_reply[req.client_id] = (req.timestamp, reply)
                await self.send_reply(req.client_id, make_reply(self.node_id, req.client_id, reply))
                
                
                if cs_txn and cs_txn.participant_cluster:
                    part_nodes = get_nodes_in_cluster(cs_txn.participant_cluster)
                    abort_msg = base_msg(MSG_2PC_ABORT, self.node_id, "participant")
                    abort_msg["txn_id"] = txn_id
                    abort_msg["request"] = req.to_dict()
                    for node in part_nodes:
                        await self.send_to_node(node, abort_msg)
        
        elif is_participant:
            
            await self.lock_mgr.release(txn.receiver, txn_id)
            
            if cs_txn:
                cs_txn.state = TxnState.ABORTED
                cs_txn.completed_at = time.time()
            
            self.logger.info(f"Participant aborted {txn_id}")
    
    def start_2pc_timeout(self, txn_id: str, phase: str = "prepare"):
        async def _timeout():
            try:
                await asyncio.sleep(TPC_TIMEOUT_MS / 1000.0)
                cs_txn = self.state.cross_shard_txns.get(txn_id)
                if cs_txn and cs_txn.is_coordinator:
                    if phase == "prepare" and not cs_txn.prepared_received:
                        self.logger.info(f"2PC timeout (prepare) for {txn_id}")
                        await self.run_abort_consensus(txn_id, cs_txn.request)
                    elif phase == "commit" and not cs_txn.ack_received:
                       
                        self.logger.info(f"2PC retrying commit for {txn_id}")
                        part_nodes = get_nodes_in_cluster(cs_txn.participant_cluster)
                        commit_msg = base_msg(MSG_2PC_COMMIT, self.node_id, "participant")
                        commit_msg["txn_id"] = txn_id
                        commit_msg["request"] = cs_txn.request.to_dict()
                        for node in part_nodes:
                            await self.send_to_node(node, commit_msg)
            except asyncio.CancelledError:
                pass
        
        if txn_id in self.tpc_timeout_tasks:
            self.tpc_timeout_tasks[txn_id].cancel()
        self.tpc_timeout_tasks[txn_id] = asyncio.create_task(_timeout())
    
    def cancel_2pc_timeout(self, txn_id: str):
        if txn_id in self.tpc_timeout_tasks:
            self.tpc_timeout_tasks[txn_id].cancel()
            del self.tpc_timeout_tasks[txn_id]
    
    
    
    async def announce_leadership(self):
        for other_cluster in get_other_clusters(self.cluster_id):
            msg = base_msg(MSG_LEADER_ANNOUNCE, self.node_id, "other_cluster")
            msg["cluster_id"] = self.cluster_id
            msg["leader_id"] = self.node_id
            msg["ballot"] = list(self.state.current_ballot)
            
            for node in get_nodes_in_cluster(other_cluster):
                await self.send_to_node(node, msg)
    
    async def handle_leader_announce(self, msg: Dict):
        cluster_id = msg["cluster_id"]
        leader_id = msg["leader_id"]
        self.state.cluster_leaders[cluster_id] = leader_id
        self.logger.info(f"Updated leader for {cluster_id}: {leader_id}")
    
  
    
    def build_accept_log(self) -> List[Dict]:
        res = []
        for s, e in self.state.log.items():
            if e.accepted and isinstance(e.value, Request):
                res.append({
                    "accept_seq": s,
                    "accept_num": list(e.ballot) if e.ballot else None,
                    "accept_val": e.value.to_dict(),
                    "flag": e.accept_flag.value,
                    "txn_id": e.txn_id
                })
        return res
    
    def build_new_view_log(self) -> List[Dict]:
        merged = {}
        for item in self.state.election_accept_log:
            s = item["accept_seq"]
            b = tuple(item["accept_num"]) if item["accept_num"] else (0, "")
            if s not in merged or b > merged[s][0]:
                merged[s] = (b, item["accept_val"], item.get("flag", AcceptFlag.NORMAL.value), item.get("txn_id"))
        
        if not merged:
            return []
        
        res = []
        for s in range(1, max(merged.keys()) + 1):
            if s in merged:
                _, val, flag, txn_id = merged[s]
                res.append({"seq": s, "kind": "request", "request": val, "flag": flag, "txn_id": txn_id})
            else:
                res.append({"seq": s, "kind": "no-op", "request": None})
        return res
    
 
    
    async def send_to_node(self, node_id: str, msg: Dict):
        if node_id not in NODE_CONFIG:
            return
        await self._send_json(NODE_CONFIG[node_id]["host"], NODE_CONFIG[node_id]["port"], msg)
    
    async def broadcast_cluster(self, msg: Dict):
        for nid in self.cluster_nodes:
            await self.send_to_node(nid, msg)
    
    async def send_reply(self, client_id: str, msg: Dict):
        await self._send_json(CONTROLLER_HOST, CONTROLLER_PORT, msg)
    
    async def _send_json(self, host: str, port: int, msg: Dict):
        await self.conn_pool.send(host, port, msg)
    
 
    async def handle_control(self, msg: Dict):
        op = msg.get("op")
        
        if op == "LF":
            
            self.state.is_failed = True
            self.logger.info("MARKED AS FAILED (LF)")
            if self.timer_task:
                self.timer_task.cancel()
            if self.heartbeat_task:
                self.heartbeat_task.cancel()
            for task in self.tpc_timeout_tasks.values():
                task.cancel()
            self.tpc_timeout_tasks.clear()
        
        elif op == "SET_LIVE":
            was_failed = self.state.is_failed
            is_live = msg["live"]
            self.state.is_failed = not is_live
            
            if was_failed and not self.state.is_failed:
                self.logger.info("RECOVERED (SET_LIVE=True)")
                self.reset_timer()
            elif not was_failed and self.state.is_failed:
                self.logger.info("MARKED FAILED (SET_LIVE=False)")
                if self.state.is_leader:
                    self.state.is_leader = False
                    if self.heartbeat_task:
                        self.heartbeat_task.cancel()
        
        elif op == "RECOVER":
            if self.state.is_failed:
                self.state.is_failed = False
                self.logger.info("RECOVERED (R)")
                self.reset_timer()
        
        elif op == "QUERY_LEADER":
            response = {
                "type": MSG_CONTROL,
                "src": self.node_id,
                "dst": "ctl",
                "op": "QUERY_LEADER_RESPONSE",
                "is_leader": self.state.is_leader and not self.state.is_failed,
                "known_leader": self.state.known_leader_id,
                "cluster_id": self.cluster_id
            }
            await self.send_reply("controller", response)
        
        elif op == "PAUSE_TIMERS":
            self.timers_paused = True
            if self.timer_task:
                self.timer_task.cancel()
            if self.heartbeat_task:
                self.heartbeat_task.cancel()
        
        elif op == "PRINT_DB":
            balances = self.db.get_modified_items()
            if balances:
                items_str = ", ".join([f"{k}:{v}" for k, v in sorted(balances.items())])
                print(f"{self.node_id}: {items_str}")
            else:
                print(f"{self.node_id}: (no modified items)")
        
        elif op == "GET_DB":
           
            balances = self.db.get_modified_items()
            self.logger.info(f"GET_DB -> {len(balances)} items")
            response = {
                "type": MSG_CONTROL,
                "src": self.node_id,
                "dst": "ctl",
                "op": "DB_DATA",
                "node_id": self.node_id,
                "cluster_id": self.cluster_id,
                "balances": balances
            }
            await self.send_reply("controller", response)
        
        elif op == "PRINT_ALL_DB":
            balances = self.db.get_all_balances()
            items_str = ", ".join([f"{k}:{v}" for k, v in sorted(balances.items())])
            print(f"{self.node_id}: {items_str}")
        
        elif op == "PRINT_BALANCE":
            account_id = int(msg["account_id"])
            balance = self.db.get_balance(account_id)
            print(f"{self.node_id} : {balance if balance is not None else 'N/A'}")
        
        elif op == "GET_BALANCE":
           
            account_id = int(msg["account_id"])
            balance = self.db.get_balance(account_id)
            self.logger.info(f"GET_BALANCE({account_id}) -> {balance}")
            response = {
                "type": MSG_CONTROL,
                "src": self.node_id,
                "dst": "ctl",
                "op": "BALANCE_DATA",
                "node_id": self.node_id,
                "cluster_id": self.cluster_id,
                "account_id": account_id,
                "balance": balance
            }
            await self.send_reply("controller", response)
            await asyncio.sleep(0.01) 
        
        elif op == "PRINT_LOG":
            self._print_log()
        
        elif op == "PRINT_VIEW":
            self._print_view()
        
        elif op == "GET_VIEW":
            
            response = {
                "type": MSG_CONTROL,
                "src": self.node_id,
                "dst": "ctl",
                "op": "VIEW_DATA",
                "node_id": self.node_id,
                "cluster_id": self.cluster_id,
                "view_history": self.state.new_view_history
            }
            await self.send_reply("controller", response)
        
        elif op == "RESET":
           
            shard_accounts = msg.get("shard_accounts")
            mapping_updates = msg.get("mapping_updates", {})
            await self._reset_state(shard_accounts, mapping_updates)
        
        elif op == "GET_DATA_FOR_RESHARD":
           
            all_data = self.db.get_all_balances()
            response = {
                "type": MSG_CONTROL,
                "src": self.node_id,
                "dst": "ctl",
                "op": "DATA_FOR_RESHARD",
                "cluster_id": self.cluster_id,
                "data": all_data
            }
            await self.send_reply("controller", response)
        
        elif op == "GET_BALANCE_FOR_RESHARD":
           
            account_id = int(msg["account_id"])
            balance = self.db.get_balance(account_id)
            response = {
                "type": MSG_CONTROL,
                "src": self.node_id,
                "dst": "ctl",
                "op": "BALANCE_FOR_RESHARD",
                "account_id": account_id,
                "balance": balance
            }
            await self.send_reply("controller", response)
        
        elif op == "UPDATE_DATA":
            #
            account_id = int(msg["account_id"])
            balance = int(msg["balance"])
            self.db.set_balance(account_id, balance)
            self.db.mark_modified(account_id)  
            self.logger.info(f"RESHARD: Received account {account_id} with balance {balance}")
        
        elif op == "DELETE_DATA":
            
            account_id = int(msg["account_id"])
            self.logger.info(f"RESHARD: Received DELETE_DATA for account {account_id}")
            cursor = self.db.conn.cursor()
            cursor.execute('DELETE FROM balances WHERE account_id = ?', (account_id,))
            deleted = cursor.rowcount
            cursor.execute('DELETE FROM modified_items WHERE account_id = ?', (account_id,))
            self.db.conn.commit()
            self.logger.info(f"RESHARD: Deleted account {account_id} (rows={deleted})")
        
        elif op == "UPDATE_SHARD_MAP":
           
            mapping_updates = msg.get("mapping_updates", {})
            for account_id_str, new_cluster in mapping_updates.items():
                account_id = int(account_id_str)
                old_cluster = SHARD_MAP.update_mapping(account_id, new_cluster)
                if old_cluster:
                    self.logger.info(f"RESHARD: Updated SHARD_MAP: account {account_id}: {old_cluster} -> {new_cluster}")
            self.logger.info(f"RESHARD: Updated {len(mapping_updates)} mappings in local SHARD_MAP")
    
    def _print_log(self):
        """Print Paxos log."""
        print(f"\n{'='*60}")
        print(f"Log for {self.node_id} ({self.cluster_id})")
        print(f"{'='*60}")
        print(f"{'Seq':<5} {'Ballot':<12} {'Flag':<6} {'Status':<6} {'Value'}")
        print(f"{'-'*60}")
        
        for s, e in sorted(self.state.log.items()):
            status = ""
            if e.accepted: status += "A"
            if e.committed: status += "C"
            if e.executed: status += "E"
            if not status: status = "X"
            
            ballot_str = f"({e.ballot[0]},{e.ballot[1]})" if e.ballot else "None"
            flag_str = e.accept_flag.value
            
            if isinstance(e.value, Request):
                txn = e.value.txn
                val = f"{txn.sender}->{txn.receiver} ${txn.amount}"
            else:
                val = str(e.value)
            
            print(f"{s:<5} {ballot_str:<12} {flag_str:<6} {status:<6} {val}")
        
        print(f"{'='*60}\n")
    
    def _print_view(self):
        print(f"\n{'='*60}")
        print(f"View History for {self.node_id}")
        print(f"Total NEW-VIEW messages: {len(self.state.new_view_history)}")
        print(f"{'='*60}")
        
        for idx, nv in enumerate(self.state.new_view_history, 1):
            print(f"\n--- NEW-VIEW #{idx} ---")
            print(f"  Ballot: {tuple(nv['ballot'])}")
            print(f"  Timestamp: {nv.get('timestamp', 'N/A')}")
            print(f"  Entries: {len(nv['accept_log'])}")
            
            if 'acks_from' in nv:
                print(f"  Role: LEADER (ACKs from: {nv['acks_from']})")
            else:
                print(f"  Role: FOLLOWER (from: {nv.get('received_from', 'unknown')})")
        
        print(f"\n{'='*60}\n")
    
    async def _reset_state(self, shard_accounts: Optional[List[int]] = None, mapping_updates: Optional[Dict[str, str]] = None):
      
        if self.timer_task:
            self.timer_task.cancel()
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
        for task in self.tpc_timeout_tasks.values():
            task.cancel()
        self.tpc_timeout_tasks.clear()
        
     
        await self.conn_pool.close_all()
        
        
        if mapping_updates:
            for account_id_str, new_cluster in mapping_updates.items():
                account_id = int(account_id_str)
                SHARD_MAP.update_mapping(account_id, new_cluster)
            self.logger.info(f"Applied {len(mapping_updates)} mapping updates from controller")
        
        
        if shard_accounts is not None:
            self.db.set_shard_accounts(shard_accounts)
        
        
        self.db.reset()
        
       
        self.lock_mgr.clear_all()
        
     
        self.state.log.clear()
        self.state.next_seq = 1
        self.state.last_executed_seq = 0
        self.state.last_reply.clear()
        self.state.accept_quorum.clear()
        self.state.cross_shard_txns.clear()
        self.state.new_view_history.clear()
        
       
        self.state.election_in_progress = False
        self.state.election_acks.clear()
        self.state.election_accept_log.clear()
        self.state.pending_prepares.clear()
        self.state.election_timer_expired = False
        
        
        initial_leader = CLUSTER_CONFIG[self.cluster_id]["initial_leader"]
        self.state.is_leader = (self.node_id == initial_leader)
        self.state.current_ballot = (1, initial_leader) if self.state.is_leader else None
        self.state.max_ballot_seen = (1, initial_leader)
        self.state.known_leader_id = initial_leader
        self.state.is_failed = False
        self.state.last_view_change_time = time.monotonic()
        
       
        for cid in CLUSTER_CONFIG:
            self.state.cluster_leaders[cid] = CLUSTER_CONFIG[cid]["initial_leader"]
        
        
        self.timers_paused = False
        
       
        if self.state.is_leader:
            self.start_heartbeats()
        else:
            self.reset_timer()
        
        self.logger.info("State reset for new test set")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Distributed Transaction Node")
    parser.add_argument("--id", required=True, help="Node ID (n1-n9)")
    parser.add_argument("--quiet", "-q", action="store_true", help="Reduce logging (only warnings/errors)")
    args = parser.parse_args()
    
    if args.quiet:
        logging.getLogger().setLevel(logging.WARNING)
    
    try:
        asyncio.run(NodeServer(args.id).start())
    except KeyboardInterrupt:
        pass