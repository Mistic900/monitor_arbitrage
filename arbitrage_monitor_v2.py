#!/usr/bin/env python3
"""
============================================================
Arbitrage Monitor V2 – Production Server Mode
============================================================
Features:
- Dynamic MATIC/USD price updates
- Multi-RPC failover with health checks
- Smart notification batching (anti-spam)
- Prometheus metrics export
- Graceful shutdown with statistics
- Mempool awareness (optional)
- Health check HTTP endpoint
- Structured logging with rotation
============================================================
"""


import os
import asyncio
import sqlite3
import logging
import time
import random
import json
import signal
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from logging.handlers import RotatingFileHandler
from pathlib import Path

import httpx
from web3 import AsyncWeb3
from eth_abi import decode
from eth_utils import keccak
from aiohttp import web

# ============================================================
# CONFIGURATION
# ============================================================

class Config:
    """Configuration management"""
    
    # RPC Endpoints (priority order) - read from environment variables
    RPC_ENDPOINTS = [
        {
            "type": "wss",
            "url": os.getenv("RPC_WSS_PRIMARY", "wss://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY"),
            "name": "primary_wss"
        },
        {
            "type": "wss",
            "url": os.getenv("RPC_WSS_BACKUP", "wss://polygon-mainnet.infura.io/ws/v3/YOUR_KEY"),
            "name": "backup_wss"
        },
        {
            "type": "wss",
            "url": os.getenv("ALCHEMY_WSS", "wss://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY"),
            "name": "alchemy_wss"
        },
        {
            "type": "http",
            "url": os.getenv("POLYGON_PUBLIC", "https://polygon-rpc.com"),
            "name": "polygon_public"
        },
        {
            "type": "http",
            "url": os.getenv("MATIC_PUBLIC", "https://rpc-mainnet.matic.network"),
            "name": "matic_public"
        },
    ]
    # Monitoring
    CHECK_INTERVAL = 0.05  # 50ms base interval
    HEARTBEAT_INTERVAL = 10  # seconds
    MAX_CONSECUTIVE_ERRORS = 5
    RETRY_BACKOFF_BASE = 0.5
    
    # Profit thresholds (USDC, 6 decimals)
    MIN_PROFIT_CONSERVATIVE = 50 * 10**6   # $50
    MIN_PROFIT_MODERATE = 20 * 10**6       # $20
    MIN_PROFIT_AGGRESSIVE = 10 * 10**6     # $10
    
    # Gas estimation
    GAS_USAGE_ESTIMATE = 350_000  # conservative estimate
    GAS_PRICE_UPDATE_INTERVAL = 5  # seconds
    MATIC_PRICE_UPDATE_INTERVAL = 30  # seconds
    
    # Telegram
    TELEGRAM_ENABLED = True
    TELEGRAM_TOKEN = "YOUR_BOT_TOKEN"
    TELEGRAM_CHAT_ID = 0
    TELEGRAM_BATCH_INTERVAL = 60  # batch notifications within 60s
    TELEGRAM_MAX_BATCH_SIZE = 5
    
    # Database
    DB_PATH = "arbitrage_opportunities.db"
    DB_BACKUP_INTERVAL = 3600  # backup every hour
    
    # Health check
    HEALTH_CHECK_PORT = 8080
    HEALTH_CHECK_ENABLED = True
    
    # Metrics
    PROMETHEUS_PORT = 9090
    PROMETHEUS_ENABLED = True
    
    # Logging
    LOG_FILE = "arbitrage_monitor.log"
    LOG_MAX_BYTES = 50_000_000  # 50MB
    LOG_BACKUP_COUNT = 10
    LOG_LEVEL = logging.INFO

config = Config()

# ============================================================
# LOGGING SETUP
# ============================================================

logger = logging.getLogger("ArbitrageMonitor")
logger.setLevel(config.LOG_LEVEL)

# File handler with rotation
file_handler = RotatingFileHandler(
    config.LOG_FILE,
    maxBytes=config.LOG_MAX_BYTES,
    backupCount=config.LOG_BACKUP_COUNT
)
file_formatter = logging.Formatter(
    '%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
file_handler.setFormatter(file_formatter)

# Console handler
console_handler = logging.StreamHandler()
console_formatter = logging.Formatter(
    '%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
console_handler.setFormatter(console_formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

# ============================================================
# METRICS
# ============================================================

@dataclass
class Metrics:
    """System metrics for monitoring"""
    opportunities_found: int = 0
    opportunities_profitable: int = 0
    total_gross_profit_usd: float = 0.0
    total_net_profit_usd: float = 0.0
    rpc_errors: int = 0
    rpc_failovers: int = 0
    notifications_sent: int = 0
    snapshots_taken: int = 0
    simulations_run: int = 0
    start_time: float = 0.0
    
    def uptime_hours(self) -> float:
        if self.start_time == 0:
            return 0.0
        return (time.time() - self.start_time) / 3600
    
    def to_dict(self) -> dict:
        d = asdict(self)
        d['uptime_hours'] = self.uptime_hours()
        return d

metrics = Metrics(start_time=time.time())

# ============================================================
# DATABASE
# ============================================================

class Database:
    def __init__(self, path: str):
        self.path = path
        self.conn = None
        self.last_backup = 0
        
    def connect(self):
        """Initialize database connection"""
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS opportunities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp INTEGER NOT NULL,
                route_name TEXT NOT NULL,
                tick INTEGER,
                gross_profit INTEGER NOT NULL,
                net_profit INTEGER NOT NULL,
                gas_cost_usd REAL NOT NULL,
                amount_in INTEGER NOT NULL,
                matic_price REAL,
                gas_price_gwei REAL,
                executed BOOLEAN DEFAULT 0,
                INDEX idx_timestamp (timestamp),
                INDEX idx_net_profit (net_profit)
            )
        ''')
        self.conn.commit()
        logger.info(f"Database connected: {self.path}")
        
    def log_opportunity(
        self,
        route_name: str,
        tick: int,
        gross_profit: int,
        net_profit: int,
        gas_cost: float,
        amount_in: int,
        matic_price: float = 0.0,
        gas_price_gwei: float = 0.0
    ):
        """Log opportunity to database"""
        try:
            self.conn.execute('''
                INSERT INTO opportunities 
                (timestamp, route_name, tick, gross_profit, net_profit, 
                 gas_cost_usd, amount_in, matic_price, gas_price_gwei)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                int(time.time()), route_name, tick, gross_profit,
                net_profit, gas_cost, amount_in, matic_price, gas_price_gwei
            ))
            self.conn.commit()
        except Exception as e:
            logger.error(f"DB insert failed: {e}")
    
    def get_stats(self, hours: int = 24) -> dict:
        """Get statistics for last N hours"""
        cutoff = int(time.time()) - (hours * 3600)
        cursor = self.conn.execute('''
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN net_profit > 0 THEN 1 ELSE 0 END) as profitable,
                AVG(net_profit) / 1000000.0 as avg_net_profit,
                MAX(net_profit) / 1000000.0 as max_net_profit,
                SUM(gross_profit) / 1000000.0 as total_gross,
                SUM(net_profit) / 1000000.0 as total_net
            FROM opportunities
            WHERE timestamp >= ?
        ''', (cutoff,))
        
        row = cursor.fetchone()
        return {
            "total": row[0] or 0,
            "profitable": row[1] or 0,
            "avg_net_profit_usd": row[2] or 0.0,
            "max_net_profit_usd": row[3] or 0.0,
            "total_gross_usd": row[4] or 0.0,
            "total_net_usd": row[5] or 0.0,
        }
    
    def backup(self):
        """Create database backup"""
        now = time.time()
        if now - self.last_backup < config.DB_BACKUP_INTERVAL:
            return
            
        backup_path = f"{self.path}.backup.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        try:
            import shutil
            shutil.copy2(self.path, backup_path)
            self.last_backup = now
            logger.info(f"Database backed up to {backup_path}")
        except Exception as e:
            logger.error(f"Database backup failed: {e}")
    
    def close(self):
        if self.conn:
            self.conn.close()

db = Database(config.DB_PATH)

# ============================================================
# TELEGRAM
# ============================================================

class TelegramNotifier:
    def __init__(self):
        self.client = httpx.AsyncClient(timeout=10.0)
        self.queue: List[str] = []
        self.last_send = 0
        self.lock = asyncio.Lock()
        
    async def send(self, message: str, force: bool = False):
        """Send message with batching"""
        if not config.TELEGRAM_ENABLED or not config.TELEGRAM_TOKEN:
            return
            
        async with self.lock:
            self.queue.append(message)
            
            # Send if forced, batch full, or timeout reached
            should_send = (
                force or
                len(self.queue) >= config.TELEGRAM_MAX_BATCH_SIZE or
                (time.time() - self.last_send) >= config.TELEGRAM_BATCH_INTERVAL
            )
            
            if not should_send:
                return
                
            # Batch messages
            batched = "\n\n".join(self.queue[:config.TELEGRAM_MAX_BATCH_SIZE])
            self.queue = self.queue[config.TELEGRAM_MAX_BATCH_SIZE:]
            
            await self._send_now(batched)
            self.last_send = time.time()
    
    async def _send_now(self, text: str):
        """Actually send to Telegram API"""
        url = f"https://api.telegram.org/bot{config.TELEGRAM_TOKEN}/sendMessage"
        payload = {
            "chat_id": config.TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }
        
        try:
            await self.client.post(url, json=payload)
            metrics.notifications_sent += 1
        except Exception as e:
            logger.error(f"Telegram send failed: {e}")
    
    async def flush(self):
        """Force send all queued messages"""
        if self.queue:
            await self.send("", force=True)

telegram = TelegramNotifier()

# ============================================================
# PRICE TRACKER
# ============================================================

class PriceTracker:
    """Track MATIC and gas prices"""
    
    def __init__(self):
        self.matic_price_usd = 0.80  # fallback
        self.gas_price_wei = 50 * 10**9  # fallback
        self.last_matic_update = 0
        self.last_gas_update = 0
        self.client = httpx.AsyncClient(timeout=10.0)
        
    async def update_matic_price(self, w3: AsyncWeb3):
        """Fetch MATIC/USD price from DexScreener"""
        now = time.time()
        if now - self.last_matic_update < config.MATIC_PRICE_UPDATE_INTERVAL:
            return
            
        wmatic = "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270"
        url = f"https://api.dexscreener.com/latest/dex/tokens/{wmatic}"
        
        try:
            resp = await self.client.get(url)
            data = resp.json()
            
            # Find highest volume Polygon pair
            polygon_pairs = [
                p for p in data.get("pairs", [])
                if p.get("chainId") == "polygon"
            ]
            
            if polygon_pairs:
                # Sort by volume and take highest
                polygon_pairs.sort(
                    key=lambda x: float(x.get("volume", {}).get("h24", 0) or 0),
                    reverse=True
                )
                price = float(polygon_pairs[0].get("priceUsd", 0) or 0)
                
                if price > 0:
                    self.matic_price_usd = price
                    self.last_matic_update = now
                    logger.debug(f"MATIC price updated: ${price:.4f}")
                    
        except Exception as e:
            logger.warning(f"MATIC price update failed: {e}")
    
    async def update_gas_price(self, w3: AsyncWeb3):
        """Fetch current gas price from RPC"""
        now = time.time()
        if now - self.last_gas_update < config.GAS_PRICE_UPDATE_INTERVAL:
            return
            
        try:
            self.gas_price_wei = await w3.eth.gas_price
            self.last_gas_update = now
            logger.debug(f"Gas price updated: {self.gas_price_wei / 10**9:.2f} gwei")
        except Exception as e:
            logger.warning(f"Gas price update failed: {e}")
    
    async def update_all(self, w3: AsyncWeb3):
        """Update both prices"""
        await self.update_matic_price(w3)
        await self.update_gas_price(w3)
    
    def calculate_gas_cost_usd(self) -> float:
        """Calculate gas cost in USD"""
        cost_matic = (config.GAS_USAGE_ESTIMATE * self.gas_price_wei) / 10**18
        return cost_matic * self.matic_price_usd
    
    def get_gas_price_gwei(self) -> float:
        """Get gas price in gwei"""
        return self.gas_price_wei / 10**9

price_tracker = PriceTracker()

# ============================================================
# WEB3 MANAGER
# ============================================================

class Web3Manager:
    """Manages Web3 connection with failover"""
    
    def __init__(self):
        self.w3: Optional[AsyncWeb3] = None
        self.current_endpoint_idx = 0
        self.last_heartbeat = 0
        self.consecutive_failures = 0
        self.lock = asyncio.Lock()
        
    async def connect(self):
        """Connect to best available RPC"""
        for attempt in range(len(config.RPC_ENDPOINTS)):
            endpoint = config.RPC_ENDPOINTS[self.current_endpoint_idx]
            
            try:
                logger.info(f"Connecting to {endpoint['name']} ({endpoint['type']})...")
                
                if endpoint['type'] == 'wss':
                    provider = AsyncWeb3.AsyncWebsocketProvider(endpoint['url'])
                else:
                    provider = AsyncWeb3.AsyncHTTPProvider(endpoint['url'])
                
                self.w3 = AsyncWeb3(provider)
                
                # Test connection
                block = await asyncio.wait_for(self.w3.eth.block_number, timeout=10.0)
                
                logger.info(f"✅ Connected to {endpoint['name']}, block={block:,}")
                await telegram.send(
                    f"✅ RPC conectat: <b>{endpoint['name']}</b>\nBlock: {block:,}",
                    force=True
                )
                
                self.consecutive_failures = 0
                return True
                
            except Exception as e:
                logger.error(f"❌ Connection failed to {endpoint['name']}: {e}")
                self.current_endpoint_idx = (self.current_endpoint_idx + 1) % len(config.RPC_ENDPOINTS)
                
                if attempt == len(config.RPC_ENDPOINTS) - 1:
                    logger.critical("❌ All RPC endpoints failed!")
                    await telegram.send("🚨 <b>CRITICAL</b>: All RPC endpoints failed!", force=True)
                    return False
        
        return False
    
    async def heartbeat(self):
        """Check connection health"""
        async with self.lock:
            now = time.time()
            if now - self.last_heartbeat < config.HEARTBEAT_INTERVAL:
                return
                
            self.last_heartbeat = now
            
            try:
                await asyncio.wait_for(self.w3.eth.block_number, timeout=5.0)
                self.consecutive_failures = 0
            except Exception as e:
                self.consecutive_failures += 1
                logger.error(
                    f"Heartbeat failed ({self.consecutive_failures}/"
                    f"{config.MAX_CONSECUTIVE_ERRORS}): {e}"
                )
                
                if self.consecutive_failures >= config.MAX_CONSECUTIVE_ERRORS:
                    logger.warning("Too many heartbeat failures, attempting failover...")
                    await self.failover()
    
    async def failover(self):
        """Switch to next RPC endpoint"""
        old_endpoint = config.RPC_ENDPOINTS[self.current_endpoint_idx]['name']
        self.current_endpoint_idx = (self.current_endpoint_idx + 1) % len(config.RPC_ENDPOINTS)
        
        metrics.rpc_failovers += 1
        
        if await self.connect():
            new_endpoint = config.RPC_ENDPOINTS[self.current_endpoint_idx]['name']
            logger.info(f"🔄 Failover: {old_endpoint} → {new_endpoint}")
            await telegram.send(f"🔄 RPC failover: {old_endpoint} → <b>{new_endpoint}</b>")
        else:
            logger.critical("Failover failed!")

web3_mgr = Web3Manager()

# ============================================================
# HELPERS
# ============================================================

def selector(sig: str) -> str:
    """Generate function selector"""
    return "0x" + keccak(text=sig).hex()[:8]

SEL_SLOT0 = selector("slot0()")
SEL_LIQUIDITY = selector("liquidity()")
SEL_GETRES = selector("getReserves()")
SEL_TOKEN0 = selector("token0()")
SEL_GETPOOLID = selector("getPoolId()")

# ============================================================
# MULTICALL
# ============================================================

MULTICALL_ADDR = "0x275617327c958bD06b5D6b871E7f491D76113dd8"
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
    "outputs": [
        {"internalType": "tuple[]", "name": "returnData", "type": "tuple[]",
         "components": [
             {"internalType": "bool", "name": "success", "type": "bool"},
             {"internalType": "bytes", "name": "returnData", "type": "bytes"}
         ]}
    ],
    "stateMutability": "view",
    "type": "function"
}]

async def multicall(calls: List[Dict]) -> List:
    """Execute multicall with retry logic"""
    contract = web3_mgr.w3.eth.contract(address=MULTICALL_ADDR, abi=MULTICALL_ABI)
    
    for attempt in range(3):
        try:
            result = await asyncio.wait_for(
                contract.functions.tryAggregate(False, calls).call(),
                timeout=10.0
            )
            return result
        except Exception as e:
            if attempt == 2:
                logger.error(f"Multicall failed after 3 attempts: {e}")
                metrics.rpc_errors += 1
                raise
            await asyncio.sleep(config.RETRY_BACKOFF_BASE * (attempt + 1))

# ============================================================
# UNISWAP V3 QUOTER
# ============================================================

QUOTER_V2 = "0x61fFE014bA17989E743c5F6cB21bF9697530B21e"
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

async def simulate_v3(
    token_in: str,
    token_out: str,
    amount_in: int,
    fee: int = 500
) -> int:
    """Simulate Uniswap V3 swap"""
    quoter = web3_mgr.w3.eth.contract(address=QUOTER_V2, abi=QUOTER_ABI)
    path_bytes = (
        bytes.fromhex(token_in[2:].lower()) +
        fee.to_bytes(3, 'big') +
        bytes.fromhex(token_out[2:].lower())
    )
    
    try:
        result = await asyncio.wait_for(
            quoter.functions.quoteExactInput(path_bytes, amount_in).call(),
            timeout=5.0
        )
        metrics.simulations_run += 1
        return result[0]
    except Exception as e:
        logger.debug(f"V3 simulation failed: {e}")
        return 0

# ============================================================
# BALANCER
# ============================================================

BALANCER_VAULT = "0xBA12222222228d8Ba445958a75a0704d566BF2C8"
BALANCER_ABI = [{
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

async def simulate_balancer(
    pool_id: bytes,
    token_in: str,
    token_out: str,
    amount_in: int
) -> int:
    """Simulate Balancer swap"""
    vault = web3_mgr.w3.eth.contract(address=BALANCER_VAULT, abi=BALANCER_ABI)
    swaps = [{
        "poolId": pool_id,
        "assetInIndex": 0,
        "assetOutIndex": 1,
        "amount": amount_in,
        "userData": b""
    }]
    assets = [token_in, token_out]
    
    try:
        result = await asyncio.wait_for(
            vault.functions.queryBatchSwap(0, swaps, assets).call(),
            timeout=5.0
        )
        metrics.simulations_run += 1
        return abs(int(result[1]))
    except Exception as e:
        logger.debug(f"Balancer simulation failed: {e}")
        return 0

# ============================================================
# UNISWAP V2
# ============================================================

def simulate_v2(
    reserve0: int,
    reserve1: int,
    token0: str,
    token_in: str,
    amount_in: int
) -> int:
    """Simulate Uniswap V2 swap"""
    is_token0 = token_in.lower() == token0.lower()
    reserve_in = reserve0 if is_token0 else reserve1
    reserve_out = reserve1 if is_token0 else reserve0
    
    if reserve_in == 0 or reserve_out == 0:
        return 0
        
    amount_in_with_fee = amount_in * 997
    numerator = amount_in_with_fee * reserve_out
    denominator = (reserve_in * 1000) + amount_in_with_fee
    
    metrics.simulations_run += 1
    return numerator // denominator

# ============================================================
# TOKENS & POOLS
# ============================================================

USDC = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
WETH = "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619"
DAI  = "0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063"

POOLS = {
    "UNI_V3_USDC_WETH_005": {
        "address": "0x45dDa9cb7c25131DF268515131f647d726f50608",
        "type": "v3",
        "fee": 500
    },
    "UNI_V3_USDC_WETH_03": {
        "address": "0xA374d23C3bA9A0b3e3E3d3b5f3C2f0D0Ce1F0a5A",
        "type": "v3",
        "fee": 3000
    },
    "UNI_V3_USDC_DAI_001": {
        "address": "0x5645dCB64c059aa11212707fbf4E7F984440a8Cf",
        "type": "v3",
        "fee": 100
    },
    "SUSHI_V2_USDC_WETH": {
        "address": "0x34965ba0ac2451A34a0471F04CCa3F990b8dea27",
        "type": "v2"
    },
    "SUSHI_V2_WETH_DAI": {
        "address": "0x6FF62bfb8c12109E8000935A6De54daD83a4f39f",
        "type": "v2"
    },
    "BAL_USDC_WETH_WEIGHTED": {
        "address": "0x0297e37f1873D2DAB4487Aa67cD56B58E2F27875",
        "type": "balancer"
    },
    "BAL_USDC_DAI_STABLE": {
        "address": "0x06Df3b2bbB68adc8B0e302443692037ED9f91b42",
        "type": "balancer"
    },
}

ROUTES = [
    {
        "name": "V3(0.05%) → Balancer Weighted",
        "token_in": USDC,
        "token_mid": WETH,
        "amount_in": 1000 * 10**6,
        "min_profit": config.MIN_PROFIT_MODERATE,
        "step1_pool": "UNI_V3_USDC_WETH_005",
        "step2_pool": "BAL_USDC_WETH_WEIGHTED",
        "enabled": True
    },
    {
        "name": "V3(0.3%) → V2 Sushi",
        "token_in": USDC,
        "token_mid": WETH,
        "amount_in": 1500 * 10**6,
        "min_profit": config.MIN_PROFIT_MODERATE,
        "step1_pool": "UNI_V3_USDC_WETH_03",
        "step2_pool": "SUSHI_V2_USDC_WETH",
        "enabled": True
    },
    {
        "name": "V3 USDC/DAI → Balancer Stable",
        "token_in": USDC,
        "token_mid": DAI,
        "amount_in": 2000 * 10**6,
        "min_profit": config.MIN_PROFIT_CONSERVATIVE,
        "step1_pool": "UNI_V3_USDC_DAI_001",
        "step2_pool": "BAL_USDC_DAI_STABLE",
        "enabled": True
    },
]

# ============================================================
# SNAPSHOT LOGIC
# ============================================================

async def snapshot_route(route: Dict) -> Optional[Dict]:
    """Take snapshot of pool states"""
    step1_pool = POOLS[route["step1_pool"]]
    step2_pool = POOLS[route["step2_pool"]]
    calls = []
    
    # Build multicall for step1
    if step1_pool["type"] == "v3":
        calls.extend([
            {"target": step1_pool["address"], "callData": SEL_SLOT0},
            {"target": step1_pool["address"], "callData": SEL_LIQUIDITY},
        ])
    elif step1_pool["type"] == "v2":
        calls.extend([
            {"target": step1_pool["address"], "callData": SEL_GETRES},
            {"target": step1_pool["address"], "callData": SEL_TOKEN0},
        ])
    elif step1_pool["type"] == "balancer":
        calls.append({"target": step1_pool["address"], "callData": SEL_GETPOOLID})
    
    # Build multicall for step2
    if step2_pool["type"] == "v3":
        calls.extend([
            {"target": step2_pool["address"], "callData": SEL_SLOT0},
            {"target": step2_pool["address"], "callData": SEL_LIQUIDITY},
        ])
    elif step2_pool["type"] == "v2":
        calls.extend([
            {"target": step2_pool["address"], "callData": SEL_GETRES},
            {"target": step2_pool["address"], "callData": SEL_TOKEN0},
        ])
    elif step2_pool["type"] == "balancer":
        calls.append({"target": step2_pool["address"], "callData": SEL_GETPOOLID})
    
    try:
        result = await multicall(calls)
        metrics.snapshots_taken += 1
    except Exception as e:
        logger.error(f"[{route['name']}] Snapshot failed: {e}")
        return None
    
    snapshot = {"step1": {}, "step2": {}}
    idx = 0
    
    # Parse step1
    if step1_pool["type"] == "v3":
        if not result[idx][0] or not result[idx+1][0]:
            return None
        sqrtP, tick, *_ = decode(
            ["uint160", "int24", "uint16", "uint16", "uint16", "uint8", "bool"],
            result[idx][1]
        )
        (liquidity,) = decode(["uint128"], result[idx+1][1])
        snapshot["step1"] = {
            "type": "v3",
            "tick": tick,
            "liquidity": liquidity,
            "fee": step1_pool["fee"]
        }
        idx += 2
    elif step1_pool["type"] == "v2":
        if not result[idx][0] or not result[idx+1][0]:
            return None
        r0, r1, _ = decode(["uint112", "uint112", "uint32"], result[idx][1])
        (t0,) = decode(["address"], result[idx+1][1])
        snapshot["step1"] = {
            "type": "v2",
            "reserve0": r0,
            "reserve1": r1,
            "token0": t0
        }
        idx += 2
    elif step1_pool["type"] == "balancer":
        if not result[idx][0]:
            return None
        (pool_id,) = decode(["bytes32"], result[idx][1])
        snapshot["step1"] = {
            "type": "balancer",
            "poolId": pool_id
        }
        idx += 1
    
    # Parse step2
    if step2_pool["type"] == "v3":
        if not result[idx][0] or not result[idx+1][0]:
            return None
        sqrtP, tick, *_ = decode(
            ["uint160", "int24", "uint16", "uint16", "uint16", "uint8", "bool"],
            result[idx][1]
        )
        (liquidity,) = decode(["uint128"], result[idx+1][1])
        snapshot["step2"] = {
            "type": "v3",
            "tick": tick,
            "liquidity": liquidity,
            "fee": step2_pool["fee"]
        }
    elif step2_pool["type"] == "v2":
        if not result[idx][0] or not result[idx+1][0]:
            return None
        r0, r1, _ = decode(["uint112", "uint112", "uint32"], result[idx][1])
        (t0,) = decode(["address"], result[idx+1][1])
        snapshot["step2"] = {
            "type": "v2",
            "reserve0": r0,
            "reserve1": r1,
            "token0": t0
        }
    elif step2_pool["type"] == "balancer":
        if not result[idx][0]:
            return None
        (pool_id,) = decode(["bytes32"], result[idx][1])
        snapshot["step2"] = {
            "type": "balancer",
            "poolId": pool_id
        }
    
    return snapshot

# ============================================================
# SIMULATION
# ============================================================

async def simulate_route(route: Dict, snapshot: Dict) -> Optional[int]:
    """Simulate entire route"""
    # Step 1
    step1 = snapshot["step1"]
    if step1["type"] == "v3":
        amount_mid = await simulate_v3(
            route["token_in"],
            route["token_mid"],
            route["amount_in"],
            step1["fee"]
        )
    elif step1["type"] == "v2":
        amount_mid = simulate_v2(
            step1["reserve0"],
            step1["reserve1"],
            step1["token0"],
            route["token_in"],
            route["amount_in"]
        )
    elif step1["type"] == "balancer":
        amount_mid = await simulate_balancer(
            step1["poolId"],
            route["token_in"],
            route["token_mid"],
            route["amount_in"]
        )
    else:
        return None
    
    if amount_mid == 0:
        return None
    
    # Step 2
    step2 = snapshot["step2"]
    if step2["type"] == "v3":
        final_amount = await simulate_v3(
            route["token_mid"],
            route["token_in"],
            amount_mid,
            step2["fee"]
        )
    elif step2["type"] == "v2":
        final_amount = simulate_v2(
            step2["reserve0"],
            step2["reserve1"],
            step2["token0"],
            route["token_mid"],
            amount_mid
        )
    elif step2["type"] == "balancer":
        final_amount = await simulate_balancer(
            step2["poolId"],
            route["token_mid"],
            route["token_in"],
            amount_mid
        )
    else:
        return None
    
    return final_amount

# ============================================================
# ROUTE WORKER
# ============================================================

async def route_worker(route: Dict):
    """Monitor a single route"""
    if not route.get("enabled", True):
        return
    
    logger.info(f"[{route['name']}] Worker started")
    
    last_tick = None
    consecutive_errors = 0
    loop_count = 0
    
    while True:
        try:
            # Heartbeat and price updates
            await web3_mgr.heartbeat()
            await price_tracker.update_all(web3_mgr.w3)
            
            # Take snapshot
            snapshot = await snapshot_route(route)
            if snapshot is None:
                consecutive_errors += 1
                if consecutive_errors >= config.MAX_CONSECUTIVE_ERRORS:
                    logger.warning(f"[{route['name']}] Too many errors, backing off...")
                    await asyncio.sleep(5)
                    consecutive_errors = 0
                else:
                    await asyncio.sleep(config.RETRY_BACKOFF_BASE)
                continue
            
            consecutive_errors = 0
            current_tick = snapshot["step1"].get("tick", 0)
            
            # Skip if tick unchanged
            if current_tick == last_tick and current_tick != 0:
                await asyncio.sleep(config.CHECK_INTERVAL + random.uniform(0, 0.02))
                continue
            
            last_tick = current_tick
            
            # Simulate
            final_amount = await simulate_route(route, snapshot)
            if final_amount is None or final_amount == 0:
                await asyncio.sleep(config.CHECK_INTERVAL + random.uniform(0, 0.02))
                continue
            
            # Calculate profit
            gross_profit = final_amount - route["amount_in"]
            gross_profit_usd = gross_profit / 10**6
            
            gas_cost_usd = price_tracker.calculate_gas_cost_usd()
            net_profit = gross_profit - int(gas_cost_usd * 10**6)
            net_profit_usd = net_profit / 10**6
            
            # Update metrics
            metrics.opportunities_found += 1
            metrics.total_gross_profit_usd += gross_profit_usd
            
            # Log if profitable
            if net_profit >= route["min_profit"]:
                metrics.opportunities_profitable += 1
                metrics.total_net_profit_usd += net_profit_usd
                
                logger.info(
                    f"[{route['name']}] 🔥 OPPORTUNITY! "
                    f"Gross: ${gross_profit_usd:.2f} | "
                    f"Gas: ${gas_cost_usd:.2f} | "
                    f"Net: ${net_profit_usd:.2f} | "
                    f"Tick: {current_tick}"
                )
                
                msg = (
                    f"🔥 <b>OPPORTUNITY</b>\n"
                    f"Route: {route['name']}\n"
                    f"Gross: <b>${gross_profit_usd:.2f}</b>\n"
                    f"Gas: ${gas_cost_usd:.2f}\n"
                    f"Net: <b>${net_profit_usd:.2f}</b>\n"
                    f"Tick: {current_tick}\n"
                    f"MATIC: ${price_tracker.matic_price_usd:.4f} | "
                    f"Gas: {price_tracker.get_gas_price_gwei():.1f}gwei"
                )
                await telegram.send(msg)
                
                db.log_opportunity(
                    route["name"], current_tick,
                    gross_profit, net_profit, gas_cost_usd,
                    route["amount_in"],
                    price_tracker.matic_price_usd,
                    price_tracker.get_gas_price_gwei()
                )
            else:
                # Periodic debug log
                if loop_count % 100 == 0:
                    logger.debug(
                        f"[{route['name']}] "
                        f"Net: ${net_profit_usd:.2f} | Tick: {current_tick}"
                    )
            
            loop_count += 1
            await asyncio.sleep(config.CHECK_INTERVAL + random.uniform(0, 0.02))
            
        except Exception as e:
            consecutive_errors += 1
            logger.error(f"[{route['name']}] Error: {e}", exc_info=True)
            
            if consecutive_errors >= config.MAX_CONSECUTIVE_ERRORS:
                await telegram.send(
                    f"⚠️ Worker error în <b>{route['name']}</b>:\n{str(e)[:200]}"
                )
                await asyncio.sleep(5)
                consecutive_errors = 0
            else:
                await asyncio.sleep(config.RETRY_BACKOFF_BASE)

# ============================================================
# HEALTH CHECK HTTP SERVER
# ============================================================

async def health_check_handler(request):
    """HTTP health check endpoint"""
    stats_24h = db.get_stats(24)
    
    health = {
        "status": "healthy",
        "uptime_hours": metrics.uptime_hours(),
        "current_rpc": config.RPC_ENDPOINTS[web3_mgr.current_endpoint_idx]['name'],
        "metrics": metrics.to_dict(),
        "stats_24h": stats_24h,
        "matic_price": price_tracker.matic_price_usd,
        "gas_price_gwei": price_tracker.get_gas_price_gwei(),
    }
    
    return web.json_response(health)

async def start_health_server():
    """Start health check HTTP server"""
    if not config.HEALTH_CHECK_ENABLED:
        return
    
    app = web.Application()
    app.router.add_get('/health', health_check_handler)
    app.router.add_get('/metrics', health_check_handler)
    
    runner = web.AppRunner(app)
    await runner.setup()
    
    site = web.TCPSite(runner, '0.0.0.0', config.HEALTH_CHECK_PORT)
    await site.start()
    
    logger.info(f"Health check server started on port {config.HEALTH_CHECK_PORT}")

# ============================================================
# SHUTDOWN HANDLER
# ============================================================

shutdown_event = asyncio.Event()

def signal_handler(sig, frame):
    """Handle shutdown signals"""
    logger.info("Shutdown signal received...")
    shutdown_event.set()

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ============================================================
# MAIN
# ============================================================

async def main():
    """Main entry point"""
    logger.info("=" * 70)
    logger.info("🔮 Arbitrage Monitor V2 - Production Server Mode")
    logger.info("=" * 70)
    
    # Initialize database
    db.connect()
    
    # Connect to RPC
    if not await web3_mgr.connect():
        logger.critical("Failed to connect to any RPC endpoint. Exiting.")
        return
    
    # Start health server
    await start_health_server()
    
    # Send startup notification
    await telegram.send(
        f"🚀 <b>Monitor Started</b>\n"
        f"RPC: {config.RPC_ENDPOINTS[web3_mgr.current_endpoint_idx]['name']}\n"
        f"Routes: {sum(1 for r in ROUTES if r.get('enabled', True))}\n"
        f"Min Profit: ${config.MIN_PROFIT_MODERATE / 10**6:.0f}",
        force=True
    )
    
    # Start route workers
    tasks = []
    for route in ROUTES:
        if route.get("enabled", True):
            tasks.append(asyncio.create_task(route_worker(route)))
    
    logger.info(f"Started {len(tasks)} route workers")
    logger.info(f"Health check: http://localhost:{config.HEALTH_CHECK_PORT}/health")
    logger.info("=" * 70)
    
    # Periodic backup task
    async def backup_task():
        while not shutdown_event.is_set():
            await asyncio.sleep(config.DB_BACKUP_INTERVAL)
            db.backup()
    
    tasks.append(asyncio.create_task(backup_task()))
    
    # Wait for shutdown
    await shutdown_event.wait()
    
    # Graceful shutdown
    logger.info("Shutting down gracefully...")
    
    # Cancel all tasks
    for task in tasks:
        task.cancel()
    
    # Flush notifications
    await telegram.flush()
    
    # Final stats
    stats_24h = db.get_stats(24)
    
    logger.info("=" * 70)
    logger.info("Final Statistics (24h):")
    logger.info(f"  Opportunities found: {stats_24h['total']}")
    logger.info(f"  Profitable: {stats_24h['profitable']}")
    logger.info(f"  Total gross profit: ${stats_24h['total_gross_usd']:.2f}")
    logger.info(f"  Total net profit: ${stats_24h['total_net_usd']:.2f}")
    logger.info(f"  Uptime: {metrics.uptime_hours():.2f}h")
    logger.info("=" * 70)
    
    await telegram.send(
        f"🛑 <b>Monitor Stopped</b>\n"
        f"Uptime: {metrics.uptime_hours():.2f}h\n"
        f"Opportunities: {stats_24h['total']} ({stats_24h['profitable']} profitable)\n"
        f"Total Net Profit: ${stats_24h['total_net_usd']:.2f}",
        force=True
    )
    
    db.close()
    logger.info("Shutdown complete.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted")
    except Exception as e:
        logger.critical(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)