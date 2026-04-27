#!/usr/bin/env python3
"""
MEV-CORE ZOMBI - Production Immortal Mode
Nu moare. Nu se oprește. Se vindecă singur.
"""

import asyncio
import os
import sqlite3
import logging
import time
import random
import gc
from typing import Dict, List, Optional
from collections import deque
from dataclasses import dataclass

import httpx
from web3 import AsyncWeb3, Web3
from eth_abi import decode
from eth_utils import keccak
from logging.handlers import RotatingFileHandler

# ============================================================
# CONFIG ZOMBI
# ============================================================

CHECK_INTERVAL = 0.05
MAX_RETRIES = 5
RETRY_BACKOFF_MAX = 30  # max 30s backoff
CIRCUIT_BREAKER_THRESHOLD = 10
CIRCUIT_BREAKER_TIMEOUT = 60
MEMORY_CLEANUP_INTERVAL = 300  # cleanup la 5 min
HEALTH_CHECK_INTERVAL = 30
AUTO_RESTART_ON_STUCK = 300  # restart dacă blocat >5min

MIN_PROFIT_AGGRESSIVE = 5 * 10**6
ESTIMATED_GAS_USAGE = 350_000
MATIC_PRICE_USD = 0.80

ENABLE_TELEGRAM = True
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = int(os.getenv("TELEGRAM_CHAT_ID", "0") or "0")

# ============================================================
# LOGGING MINIMAL (stealth când merge)
# ============================================================

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)  # doar warnings+errors

fmt = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
fh = RotatingFileHandler('zombi.log', maxBytes=10_000_000, backupCount=3)
fh.setFormatter(fmt)
logger.addHandler(fh)

# Console doar pentru criticals
console = logging.StreamHandler()
console.setLevel(logging.CRITICAL)
console.setFormatter(fmt)
logger.addHandler(console)

def log_info(msg: str):
    """Silent info - doar în file"""
    pass  # no-op în production

def log_warn(msg: str):
    logger.warning(msg)

def log_crit(msg: str):
    logger.critical(msg)

# ============================================================
# DB ZOMBI (auto-recovery)
# ============================================================

class ZombiDB:
    def __init__(self, path: str = 'zombi_opps.db'):
        self.path = path
        self.conn = None
        self._reconnect()
    
    def _reconnect(self):
        try:
            if self.conn:
                self.conn.close()
        except:
            pass
        
        self.conn = sqlite3.connect(self.path, check_same_thread=False, timeout=30)
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS opps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER,
                route TEXT,
                tick INTEGER,
                gross INTEGER,
                net INTEGER,
                gas REAL,
                amt INTEGER
            )
        ''')
        self.conn.commit()
    
    def log(self, route: str, tick: int, gross: int, net: int, gas: float, amt: int):
        for attempt in range(3):
            try:
                self.conn.execute(
                    'INSERT INTO opps (ts,route,tick,gross,net,gas,amt) VALUES (?,?,?,?,?,?,?)',
                    (int(time.time()), route, tick, gross, net, gas, amt)
                )
                self.conn.commit()
                return
            except Exception as e:
                if attempt == 2:
                    log_warn(f"DB log failed: {e}")
                    self._reconnect()
                else:
                    time.sleep(0.1)

db = ZombiDB()

# ============================================================
# TELEGRAM ZOMBI (never fails)
# ============================================================

_telegram_queue = deque(maxlen=100)
_telegram_last_send = 0
_telegram_client = None

def _get_tg_client():
    global _telegram_client
    if _telegram_client is None:
        _telegram_client = httpx.AsyncClient(timeout=5.0)
    return _telegram_client

async def telegram_send_zombi(msg: str, force: bool = False):
    if not ENABLE_TELEGRAM or not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    
    global _telegram_last_send
    now = time.time()
    
    # Batch: max 1 msg per 10s unless forced
    if not force and (now - _telegram_last_send) < 10:
        _telegram_queue.append(msg)
        return
    
    # Flush queue
    if _telegram_queue:
        msg = "\n\n".join(list(_telegram_queue)[-3:]) + "\n\n" + msg
        _telegram_queue.clear()
    
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": msg[:4000], "parse_mode": "HTML"}
    
    try:
        client = _get_tg_client()
        await client.post(url, json=payload)
        _telegram_last_send = now
    except:
        pass  # never crash on telegram

# ============================================================
# CIRCUIT BREAKER
# ============================================================

class CircuitBreaker:
    def __init__(self, threshold: int, timeout: int):
        self.threshold = threshold
        self.timeout = timeout
        self.failures = 0
        self.last_failure = 0
        self.state = 'closed'  # closed, open, half_open
    
    def record_success(self):
        self.failures = 0
        self.state = 'closed'
    
    def record_failure(self):
        self.failures += 1
        self.last_failure = time.time()
        if self.failures >= self.threshold:
            self.state = 'open'
            log_warn(f"Circuit OPEN (failures={self.failures})")
    
    def can_attempt(self) -> bool:
        if self.state == 'closed':
            return True
        
        if self.state == 'open':
            if time.time() - self.last_failure > self.timeout:
                self.state = 'half_open'
                self.failures = 0
                return True
            return False
        
        # half_open
        return True

# ============================================================
# WEB3 ZOMBI (ultra-aggressive failover)
# ============================================================

def _wss_to_https(url: str) -> str:
    if url.startswith("wss://"):
        return "https://" + url[len("wss://"):]
    return url

class ZombiWeb3:
    def __init__(self):
        self.w3: Optional[AsyncWeb3] = None
        self.endpoints = self._load_endpoints()
        self.current_idx = 0
        self.circuit_breakers = {i: CircuitBreaker(CIRCUIT_BREAKER_THRESHOLD, CIRCUIT_BREAKER_TIMEOUT) 
                                  for i in range(len(self.endpoints))}
        self.last_heartbeat = 0
        self.consecutive_fails = 0
    
    def _load_endpoints(self) -> List[str]:
        urls = []
        for var in ["RPC_WSS_PRIMARY", "RPC_WSS_BACKUP", "ALCHEMY_WSS", "POLYGON_PUBLIC"]:
            raw = os.getenv(var, "").strip()
            if raw:
                urls.append(_wss_to_https(raw))
        
        # Fallbacks hardcore
        urls.extend([
            "https://polygon-rpc.com",
            "https://rpc-mainnet.matic.network",
            "https://polygon-bor.publicnode.com",
        ])
        
        return urls
    
    async def init(self):
        for i in range(len(self.endpoints)):
            if await self._try_connect(i):
                return
        
        # Dacă ajunge aici, ALL failed - wait și retry
        log_crit("ALL RPC FAILED - retrying in 30s...")
        await asyncio.sleep(30)
        await self.init()  # recursive retry
    
    async def _try_connect(self, idx: int) -> bool:
        url = self.endpoints[idx]
        cb = self.circuit_breakers[idx]
        
        if not cb.can_attempt():
            return False
        
        try:
            self.w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(url, request_kwargs={'timeout': 10}))
            block = await asyncio.wait_for(self.w3.eth.block_number, timeout=15)
            cb.record_success()
            self.current_idx = idx
            self.consecutive_fails = 0
            log_info(f"RPC connected: {url[:50]}, block={block}")
            await telegram_send_zombi(f"✅ RPC: {url[:40]}")
            return True
        except Exception as e:
            cb.record_failure()
            return False
    
    async def heartbeat(self):
        now = time.time()
        if now - self.last_heartbeat < HEALTH_CHECK_INTERVAL:
            return
        
        self.last_heartbeat = now
        
        try:
            await asyncio.wait_for(self.w3.eth.block_number, timeout=10)
            self.consecutive_fails = 0
        except:
            self.consecutive_fails += 1
            if self.consecutive_fails >= 3:
                await self._failover()
    
    async def _failover(self):
        log_warn("RPC failover triggered")
        
        # Try next endpoint
        for offset in range(1, len(self.endpoints)):
            next_idx = (self.current_idx + offset) % len(self.endpoints)
            if await self._try_connect(next_idx):
                await telegram_send_zombi(f"🔄 Failover → {self.endpoints[next_idx][:40]}")
                return
        
        # ALL failed again - nuclear option: wait and retry all
        log_crit("Failover exhausted - waiting 60s")
        await asyncio.sleep(60)
        await self.init()

web3_zombi = ZombiWeb3()

def get_w3() -> AsyncWeb3:
    return web3_zombi.w3

# ============================================================
# MULTICALL ZOMBI
# ============================================================

MULTICALL = "0x275617327c958bD06b5D6b871E7f491D76113dd8"
MULTICALL_ABI = [{
    "inputs": [
        {"internalType": "bool", "name": "requireSuccess", "type": "bool"},
        {"internalType": "tuple[]", "name": "calls", "type": "tuple[]",
         "components": [
             {"internalType": "address", "name": "target", "type": "address"},
             {"internalType": "bytes", "name": "callData", "type": "bytes"}
         ]}
    ],
    "name": "tryAggregate",
    "outputs": [{"internalType": "tuple[]", "name": "returnData", "type": "tuple[]",
         "components": [
             {"internalType": "bool", "name": "success", "type": "bool"},
             {"internalType": "bytes", "name": "returnData", "type": "bytes"}
         ]}],
    "stateMutability": "view",
    "type": "function"
}]

async def multicall_zombi(calls: List[Dict]) -> Optional[List]:
    contract = get_w3().eth.contract(address=MULTICALL, abi=MULTICALL_ABI)
    formatted = [(c["target"], c["callData"]) for c in calls]
    
    for attempt in range(MAX_RETRIES):
        try:
            result = await asyncio.wait_for(
                contract.functions.tryAggregate(False, formatted).call(),
                timeout=15
            )
            return result
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                return None
            
            backoff = min(2 ** attempt, RETRY_BACKOFF_MAX)
            await asyncio.sleep(backoff + random.uniform(0, 1))
    
    return None

# ============================================================
# HELPERS
# ============================================================

def sel(sig: str) -> str:
    return "0x" + keccak(text=sig).hex()[:8]

SEL_SLOT0 = sel("slot0()")
SEL_LIQ = sel("liquidity()")
SEL_RES = sel("getReserves()")
SEL_T0 = sel("token0()")
SEL_PID = sel("getPoolId()")

def to_bytes(s: str) -> bytes:
    return bytes.fromhex(s[2:])

# ============================================================
# QUOTER V3 ZOMBI
# ============================================================

QUOTER = "0x61fFE014bA17989E743c5F6cB21bF9697530B21e"
QUOTER_ABI = [{
    "inputs": [
        {"internalType": "bytes", "name": "path", "type": "bytes"},
        {"internalType": "uint256", "name": "amountIn", "type": "uint256"}
    ],
    "name": "quoteExactInput",
    "outputs": [
        {"internalType": "uint256", "name": "amountOut", "type": "uint256"},
        {"internalType": "uint160[]", "name": "sqrtPriceX96AfterList", "type": "uint160[]"},
        {"internalType": "uint32[]", "name": "initializedTicksCrossedList", "type": "uint32[]"},
        {"internalType": "uint256", "name": "gasEstimate", "type": "uint256"}
    ],
    "stateMutability": "nonpayable",
    "type": "function"
}]

async def sim_v3(tin: str, tout: str, amt: int, fee: int = 500) -> int:
    try:
        quoter = get_w3().eth.contract(address=QUOTER, abi=QUOTER_ABI)
        path = bytes.fromhex(tin[2:].lower()) + fee.to_bytes(3, 'big') + bytes.fromhex(tout[2:].lower())
        result = await asyncio.wait_for(quoter.functions.quoteExactInput(path, amt).call(), timeout=10)
        return result[0]
    except:
        return 0

# ============================================================
# BALANCER ZOMBI
# ============================================================

BAL_VAULT = "0xBA12222222228d8Ba445958a75a0704d566BF2C8"
BAL_ABI = [{
    "name": "queryBatchSwap",
    "type": "function",
    "stateMutability": "view",
    "inputs": [
        {"name": "kind", "type": "uint8"},
        {"name": "swaps", "type": "tuple[]",
         "components": [
             {"name": "poolId", "type": "bytes32"},
             {"name": "assetInIndex", "type": "uint256"},
             {"name": "assetOutIndex", "type": "uint256"},
             {"name": "amount", "type": "uint256"},
             {"name": "userData", "type": "bytes"}
         ]},
        {"name": "assets", "type": "address[]"}
    ],
    "outputs": [{"name": "", "type": "int256[]"}]
}]

async def sim_bal(pid: bytes, tin: str, tout: str, amt: int) -> int:
    try:
        vault = get_w3().eth.contract(address=BAL_VAULT, abi=BAL_ABI)

        swaps = [{
            "poolId": pid,
            "assetInIndex": 0,
            "assetOutIndex": 1,
            "amount": amt,
            "userData": b""
        }]

        result = await asyncio.wait_for(
            vault.functions.queryBatchSwap(0, swaps, [tin, tout]).call(),
            timeout=10
        )

        # Web3 returnează (int256[],) → extragem vectorul
        if isinstance(result, (list, tuple)):
            deltas = result[0]
        else:
            deltas = result

        # deltas[0] = delta pentru tin, deltas[1] = delta pentru tout (negativ)
        return abs(int(deltas[1]))
    except Exception:
        return 0

# ============================================================
# V2 ZOMBI
# ============================================================

def sim_v2(r0: int, r1: int, t0: str, tin: str, amt: int) -> int:
    if r0 == 0 or r1 == 0:
        return 0
    
    is_t0 = tin.lower() == t0.lower()
    rin = r0 if is_t0 else r1
    rout = r1 if is_t0 else r0
    
    amt_fee = amt * 997
    num = amt_fee * rout
    den = (rin * 1000) + amt_fee
    return num // den

# ============================================================
# GAS ZOMBI
# ============================================================

class ZombiGas:
    def __init__(self):
        self.gas_price = 50 * 10**9
        self.last_update = 0
    
    async def get(self) -> int:
        now = time.time()
        if now - self.last_update > 10:
            try:
                self.gas_price = await asyncio.wait_for(get_w3().eth.gas_price, timeout=5)
                self.last_update = now
            except:
                pass  # keep old value
        return self.gas_price
    
    async def cost_usd(self) -> float:
        gp = await self.get()
        return (ESTIMATED_GAS_USAGE * gp / 10**18) * MATIC_PRICE_USD

gas_zombi = ZombiGas()

# ============================================================
# POOLS & ROUTES
# ============================================================

USDC = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
WETH = "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619"
DAI = "0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063"

POOLS = {
    "U3_005": {"addr": Web3.to_checksum_address("0x45dDa9cb7c25131DF268515131f647d726f50608"), "type": "v3", "fee": 500},
    "U3_03": {"addr": Web3.to_checksum_address("0xA374094527e1673A86dE625aa59517c5dE346d32"), "type": "v3", "fee": 3000},
    "U3_001": {"addr": Web3.to_checksum_address("0x5645dCB64c059aa11212707fbf4E7F984440a8Cf"), "type": "v3", "fee": 100},
    "SU2": {"addr": Web3.to_checksum_address("0x34965ba0ac2451A34a0471F04CCa3F990b8dea27"), "type": "v2"},
    "BAL_W": {"addr": Web3.to_checksum_address("0x0297e37f1873D2DAB4487Aa67cD56B58E2F27875"), "type": "bal"},
    "BAL_S": {"addr": Web3.to_checksum_address("0x06Df3b2bbB68adc8B0e302443692037ED9f91b42"), "type": "bal"},
}

ROUTES = [
    {"n": "R1", "tin": USDC, "tm": WETH, "amt": 2500*10**6, "mp": 10*10**6, "p1": "U3_005", "p2": "U3_03", "on": True},
    {"n": "R2", "tin": USDC, "tm": WETH, "amt": 3000*10**6, "mp": 12*10**6, "p1": "U3_03", "p2": "BAL_W", "on": True},
    {"n": "R3", "tin": USDC, "tm": WETH, "amt": 2000*10**6, "mp": 8*10**6, "p1": "U3_005", "p2": "SU2", "on": True},
    {"n": "R4", "tin": USDC, "tm": DAI, "amt": 5000*10**6, "mp": 15*10**6, "p1": "U3_001", "p2": "BAL_S", "on": True},
    {"n": "R5", "tin": USDC, "tm": DAI, "amt": 4000*10**6, "mp": 12*10**6, "p1": "BAL_S", "p2": "U3_001", "on": True},
]

# ============================================================
# SNAPSHOT ZOMBI (global batched)
# ============================================================

SNAPSHOTS = {}
SNAP_LOCK = asyncio.Lock()

def build_calls(r: Dict) -> List[Dict]:
    p1 = POOLS[r["p1"]]
    p2 = POOLS[r["p2"]]
    calls = []
    
    # p1
    if p1["type"] == "v3":
        calls.extend([{"target": p1["addr"], "callData": to_bytes(SEL_SLOT0)},
                      {"target": p1["addr"], "callData": to_bytes(SEL_LIQ)}])
    elif p1["type"] == "v2":
        calls.extend([{"target": p1["addr"], "callData": to_bytes(SEL_RES)},
                      {"target": p1["addr"], "callData": to_bytes(SEL_T0)}])
    elif p1["type"] == "bal":
        calls.append({"target": p1["addr"], "callData": to_bytes(SEL_PID)})
    
    # p2
    if p2["type"] == "v3":
        calls.extend([{"target": p2["addr"], "callData": to_bytes(SEL_SLOT0)},
                      {"target": p2["addr"], "callData": to_bytes(SEL_LIQ)}])
    elif p2["type"] == "v2":
        calls.extend([{"target": p2["addr"], "callData": to_bytes(SEL_RES)},
                      {"target": p2["addr"], "callData": to_bytes(SEL_T0)}])
    elif p2["type"] == "bal":
        calls.append({"target": p2["addr"], "callData": to_bytes(SEL_PID)})
    
    return calls

def parse_snap(r: Dict, res: List) -> Optional[Dict]:
    p1 = POOLS[r["p1"]]
    p2 = POOLS[r["p2"]]
    s = {"s1": {}, "s2": {}}
    idx = 0
    
    # p1
    if p1["type"] == "v3":
        if not res[idx][0] or not res[idx+1][0]:
            return None
        _, tick, *_ = decode(["uint160","int24","uint16","uint16","uint16","uint8","bool"], res[idx][1])
        (liq,) = decode(["uint128"], res[idx+1][1])
        s["s1"] = {"type": "v3", "tick": tick, "liq": liq, "fee": p1["fee"]}
        idx += 2
    elif p1["type"] == "v2":
        if not res[idx][0] or not res[idx+1][0]:
            return None
        r0, r1, _ = decode(["uint112","uint112","uint32"], res[idx][1])
        (t0,) = decode(["address"], res[idx+1][1])
        s["s1"] = {"type": "v2", "r0": r0, "r1": r1, "t0": t0}
        idx += 2
    elif p1["type"] == "bal":
        if not res[idx][0]:
            return None
        (pid,) = decode(["bytes32"], res[idx][1])
        s["s1"] = {"type": "bal", "pid": pid}
        idx += 1
    
    # p2
    if p2["type"] == "v3":
        if not res[idx][0] or not res[idx+1][0]:
            return None
        _, tick, *_ = decode(["uint160","int24","uint16","uint16","uint16","uint8","bool"], res[idx][1])
        (liq,) = decode(["uint128"], res[idx+1][1])
        s["s2"] = {"type": "v3", "tick": tick, "liq": liq, "fee": p2["fee"]}
    elif p2["type"] == "v2":
        if not res[idx][0] or not res[idx+1][0]:
            return None
        r0, r1, _ = decode(["uint112","uint112","uint32"], res[idx][1])
        (t0,) = decode(["address"], res[idx+1][1])
        s["s2"] = {"type": "v2", "r0": r0, "r1": r1, "t0": t0}
    elif p2["type"] == "bal":
        if not res[idx][0]:
            return None
        (pid,) = decode(["bytes32"], res[idx][1])
        s["s2"] = {"type": "bal", "pid": pid}
    
    return s

async def snapshot_loop():
    """Global batched snapshot - IMMORTAL"""
    failures = 0
    
    while True:
        try:
            await web3_zombi.heartbeat()
            
            all_calls = []
            ranges = []
            
            for r in ROUTES:
                if not r.get("on"):
                    continue
                calls = build_calls(r)
                start = len(all_calls)
                all_calls.extend(calls)
                ranges.append((r["n"], start, len(all_calls)))
            
            if not all_calls:
                await asyncio.sleep(CHECK_INTERVAL)
                continue
            
            result = await multicall_zombi(all_calls)
            
            if result is None:
                failures += 1
                if failures > 50:
                    log_warn(f"Snapshot failures: {failures}")
                    failures = 0
                await asyncio.sleep(CHECK_INTERVAL * 2)
                continue
            
            failures = 0
            
            async with SNAP_LOCK:
                for name, start, end in ranges:
                    route = next(r for r in ROUTES if r["n"] == name)
                    snap = parse_snap(route, result[start:end])
                    SNAPSHOTS[name] = snap
            
        except Exception as e:
            failures += 1
            if failures % 20 == 0:
                log_warn(f"Snapshot loop error: {e}")
        
        await asyncio.sleep(CHECK_INTERVAL)

# ============================================================
# SIMULATION ZOMBI
# ============================================================

async def sim_route(r: Dict, s: Dict) -> Optional[int]:
    try:
        # Step 1
        s1 = s["s1"]
        if s1["type"] == "v3":
            mid = await sim_v3(r["tin"], r["tm"], r["amt"], s1["fee"])
        elif s1["type"] == "v2":
            mid = sim_v2(s1["r0"], s1["r1"], s1["t0"], r["tin"], r["amt"])
        elif s1["type"] == "bal":
            mid = await sim_bal(s1["pid"], r["tin"], r["tm"], r["amt"])
        else:
            return None
        
        if mid == 0:
            return None
        
        # Step 2
        s2 = s["s2"]
        if s2["type"] == "v3":
            final = await sim_v3(r["tm"], r["tin"], mid, s2["fee"])
        elif s2["type"] == "v2":
            final = sim_v2(s2["r0"], s2["r1"], s2["t0"], r["tm"], mid)
        elif s2["type"] == "bal":
            final = await sim_bal(s2["pid"], r["tm"], r["tin"], mid)
        else:
            return None
        
        return final
    except:
        return None

# ============================================================
# WORKER ZOMBI (never dies)
# ============================================================

async def worker_zombi(r: Dict):
    if not r.get("on"):
        return
    
    name = r["n"]
    last_tick = None
    stuck_counter = 0
    last_activity = time.time()
    
    while True:
        try:
            # Anti-stuck mechanism
            if time.time() - last_activity > AUTO_RESTART_ON_STUCK:
                log_warn(f"[{name}] STUCK detected - auto-restart")
                await telegram_send_zombi(f"⚠️ [{name}] auto-restart (stuck)")
                last_activity = time.time()
                stuck_counter = 0
                continue
            
            async with SNAP_LOCK:
                snap = SNAPSHOTS.get(name)
            
            if snap is None:
                await asyncio.sleep(CHECK_INTERVAL)
                continue
            
            tick = snap["s1"].get("tick", 0)
            
            if tick == last_tick and tick != 0:
                stuck_counter += 1
                if stuck_counter > 1000:
                    stuck_counter = 0
                    last_tick = None  # force refresh
                await asyncio.sleep(CHECK_INTERVAL + random.uniform(0, 0.02))
                continue
            
            last_tick = tick
            stuck_counter = 0
            last_activity = time.time()
            
            final = await sim_route(r, snap)
            
            if final is None or final == 0:
                await asyncio.sleep(CHECK_INTERVAL + random.uniform(0, 0.02))
                continue
            
            gross = final - r["amt"]
            gross_usd = gross / 10**6
            
            gas_usd = await gas_zombi.cost_usd()
            net = gross - int(gas_usd * 10**6)
            net_usd = net / 10**6
            
            if net >= r["mp"]:
                log_warn(f"[{name}] 🔥 ${net_usd:.2f} | tick={tick}")
                
                msg = f"🔥 <b>{name}</b>\nNet: ${net_usd:.2f}\nGas: ${gas_usd:.2f}\nTick: {tick}"
                await telegram_send_zombi(msg)
                
                db.log(name, tick, gross, net, gas_usd, r["amt"])
            
            await asyncio.sleep(CHECK_INTERVAL + random.uniform(0, 0.02))
            
        except Exception as e:
            # Never crash - just log and continue
            if random.random() < 0.01:  # log 1% of errors
                log_warn(f"[{name}] worker error: {e}")
            await asyncio.sleep(CHECK_INTERVAL)

# ============================================================
# MEMORY CLEANUP ZOMBI
# ============================================================

async def memory_cleanup_loop():
    """Periodic garbage collection"""
    while True:
        await asyncio.sleep(MEMORY_CLEANUP_INTERVAL)
        gc.collect()

# ============================================================
# MAIN ZOMBI
# ============================================================

async def main_zombi():
    log_crit("ZOMBI MODE ACTIVATED")
    
    # Init RPC with retries
    await web3_zombi.init()
    
    await telegram_send_zombi("🧟 ZOMBI v3 PORNIT", force=True)
    
    tasks = [asyncio.create_task(snapshot_loop())]
    tasks.append(asyncio.create_task(memory_cleanup_loop()))
    
    for r in ROUTES:
        if r.get("on"):
            tasks.append(asyncio.create_task(worker_zombi(r)))
    
    log_crit(f"ZOMBI running: {len(tasks)} tasks")
    
    # IMMORTAL LOOP
    while True:
        try:
            await asyncio.gather(*tasks)
        except KeyboardInterrupt:
            log_crit("Manual stop")
            await telegram_send_zombi("🛑 ZOMBI stopped manually", force=True)
            break
        except Exception as e:
            # NEVER DIE - restart tasks
            log_crit(f"ZOMBI resurrection: {e}")
            await telegram_send_zombi(f"🧟 Auto-recovery: {str(e)[:100]}", force=True)
            await asyncio.sleep(10)
            # Tasks will auto-restart from their own loops

if __name__ == "__main__":
    try:
        asyncio.run(main_zombi())
    except KeyboardInterrupt:
        log_crit("ZOMBI terminated")
    except Exception as e:
        log_crit(f"ZOMBI died (impossible): {e}")
        # Nuclear restart
        import sys
        import subprocess
        subprocess.Popen([sys.executable] + sys.argv)