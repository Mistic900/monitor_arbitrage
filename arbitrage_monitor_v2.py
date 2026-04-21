import asyncio
import sqlite3
import logging
import time
import random
from datetime import datetime
from typing import Dict, List, Optional

import httpx
from web3 import AsyncWeb3
from eth_abi import decode
from eth_utils import keccak

# ============================================================
# CONFIGURATION
# ============================================================

# RPC endpoints
RPC_WSS_PRIMARY = "wss://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY"
RPC_WSS_BACKUP  = "wss://polygon-mainnet.infura.io/ws/v3/YOUR_KEY"
RPC_HTTP_FALLBACK = "https://polygon-rpc.com"

# Monitoring Configuration
CHECK_INTERVAL = 0.05  # base interval
MAX_RETRIES = 3
RETRY_BACKOFF = 0.5  # seconds

# Profit Configuration (în USDC, 6 decimals)
MIN_PROFIT_CONSERVATIVE = 50 * 10**6   # $50 USDC
MIN_PROFIT_MODERATE = 20 * 10**6       # $20 USDC
MIN_PROFIT_AGGRESSIVE = 10 * 10**6     # $10 USDC

# Gas Configuration (pentru Polygon)
ESTIMATED_GAS_USAGE = 350_000  # ~350k gas per arbitrage
MATIC_PRICE_USD = 0.80  # Update this periodically!

# Telegram Notifications
ENABLE_TELEGRAM = True
TELEGRAM_TOKEN = "PUNE_TOKENUL_TAU_AICI"
TELEGRAM_CHAT_ID = 0  # pune chat_id aici

# ============================================================
# LOGGING SETUP (rotativ, server-grade)
# ============================================================

from logging.handlers import RotatingFileHandler

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

fmt = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')

fh = RotatingFileHandler('arbitrage_monitor.log', maxBytes=50_000_000, backupCount=5)
fh.setFormatter(fmt)
sh = logging.StreamHandler()
sh.setFormatter(fmt)

logger.addHandler(fh)
logger.addHandler(sh)

# ============================================================
# DATABASE SETUP
# ============================================================

db = sqlite3.connect('arbitrage_opportunities.db')
db.execute('''
    CREATE TABLE IF NOT EXISTS opportunities (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER,
        route_name TEXT,
        tick INTEGER,
        gross_profit INTEGER,
        net_profit INTEGER,
        gas_cost_usd REAL,
        amount_in INTEGER,
        executed BOOLEAN DEFAULT 0
    )
''')
db.commit()

def log_opportunity(route_name: str, tick: int, gross_profit: int, 
                   net_profit: int, gas_cost: float, amount_in: int):
    db.execute('''
        INSERT INTO opportunities 
        (timestamp, route_name, tick, gross_profit, net_profit, gas_cost_usd, amount_in)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (int(time.time()), route_name, tick, gross_profit, net_profit, gas_cost, amount_in))
    db.commit()

# ============================================================
# TELEGRAM MODULE
# ============================================================

async def telegram_send(message: str):
    if not ENABLE_TELEGRAM or not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }

    try:
        async with httpx.AsyncClient(timeout=5.0) as tg:
            await tg.post(url, json=payload)
    except Exception as e:
        logger.error(f"Telegram send failed: {e}")

# ============================================================
# WEB3 SETUP – WSS + fallback
# ============================================================

class Web3Manager:
    def __init__(self):
        self.current_rpc = "primary_wss"
        self.w3 = None
        self.last_heartbeat = 0
        self.heartbeat_interval = 10  # sec
        self.lock = asyncio.Lock()

    async def init(self):
        await self._connect("primary_wss")

    async def _connect(self, which: str):
        if which == "primary_wss":
            logger.info("Connecting to PRIMARY WSS...")
            self.w3 = AsyncWeb3(AsyncWeb3.AsyncWebsocketProvider(RPC_WSS_PRIMARY))
            self.current_rpc = "primary_wss"
        elif which == "backup_wss":
            logger.info("Connecting to BACKUP WSS...")
            self.w3 = AsyncWeb3(AsyncWeb3.AsyncWebsocketProvider(RPC_WSS_BACKUP))
            self.current_rpc = "backup_wss"
        elif which == "http":
            logger.info("Connecting to HTTP fallback...")
            self.w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(RPC_HTTP_FALLBACK))
            self.current_rpc = "http"
        else:
            raise ValueError("Unknown RPC type")

        # test connection
        try:
            block = await self.w3.eth.block_number
            logger.info(f"✅ Connected via {self.current_rpc}, block={block}")
            await telegram_send(f"✅ RPC conectat: <b>{self.current_rpc}</b>, block={block}")
        except Exception as e:
            logger.error(f"RPC connect failed ({which}): {e}")
            raise

    async def heartbeat(self):
        async with self.lock:
            now = time.time()
            if now - self.last_heartbeat < self.heartbeat_interval:
                return
            self.last_heartbeat = now

            try:
                _ = await self.w3.eth.block_number
            except Exception as e:
                logger.error(f"Heartbeat failed on {self.current_rpc}: {e}")
                await telegram_send(f"⚠️ Heartbeat failed pe <b>{self.current_rpc}</b>: {e}")
                await self._failover()

    async def _failover(self):
        order = ["primary_wss", "backup_wss", "http"]
        try_next = {
            "primary_wss": "backup_wss",
            "backup_wss": "http",
            "http": "primary_wss"
        }
        next_rpc = try_next[self.current_rpc]
        try:
            await self._connect(next_rpc)
            await telegram_send(f"🔄 RPC failover → <b>{self.current_rpc}</b>")
        except Exception as e:
            logger.error(f"Failover to {next_rpc} failed: {e}")
            # ultima redută: HTTP
            if next_rpc != "http":
                await self._connect("http")

web3_manager = Web3Manager()

# shortcut
def get_w3() -> AsyncWeb3:
    return web3_manager.w3

client = httpx.AsyncClient(timeout=10.0)

# ============================================================
# HELPERS
# ============================================================

def selector(sig: str) -> str:
    return "0x" + keccak(text=sig).hex()[:8]

SEL_SLOT0 = selector("slot0()")
SEL_LIQUIDITY = selector("liquidity()")
SEL_GETRES = selector("getReserves()")
SEL_TOKEN0 = selector("token0()")
SEL_GETPOOLID = selector("getPoolId()")

# ============================================================
# MULTICALL
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
    contract = get_w3().eth.contract(address=MULTICALL, abi=MULTICALL_ABI)
    for attempt in range(MAX_RETRIES):
        try:
            result = await contract.functions.tryAggregate(False, calls).call()
            return result
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                logger.error(f"Multicall failed after {MAX_RETRIES} attempts: {e}")
                raise
            logger.warning(f"Multicall attempt {attempt + 1} failed: {e}")
            await asyncio.sleep(RETRY_BACKOFF * (attempt + 1))

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

async def simulate_v3_accurate(token_in: str, token_out: str, amount_in: int, fee: int = 500) -> int:
    quoter = get_w3().eth.contract(address=QUOTER_V2, abi=QUOTER_ABI)
    path_bytes = (
        bytes.fromhex(token_in[2:].lower()) +
        fee.to_bytes(3, 'big') +
        bytes.fromhex(token_out[2:].lower())
    )
    try:
        result = await quoter.functions.quoteExactInput(path_bytes, amount_in).call()
        amount_out = result[0]
        if amount_out == 0:
            logger.debug(f"V3 quote returned 0 for {amount_in} {token_in[:6]}→{token_out[:6]}")
            return 0
        return amount_out
    except Exception as e:
        logger.error(f"V3 simulation failed: {e}")
        return 0

# ============================================================
# BALANCER
# ============================================================

BALANCER_VAULT = "0xBA12222222228d8Ba445958a75a0704d566BF2C8"
BALANCER_VAULT_ABI = [{
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

async def simulate_balancer(pool_id: bytes, token_in: str, token_out: str, amount_in: int) -> int:
    vault = get_w3().eth.contract(address=BALANCER_VAULT, abi=BALANCER_VAULT_ABI)
    swaps = [{
        "poolId": pool_id,
        "assetInIndex": 0,
        "assetOutIndex": 1,
        "amount": amount_in,
        "userData": b""
    }]
    assets = [token_in, token_out]
    try:
        result = await vault.functions.queryBatchSwap(0, swaps, assets).call()
        amount_out = abs(int(result[1]))
        if amount_out == 0:
            logger.debug(f"Balancer quote returned 0 for poolId {pool_id.hex()[:10]}")
            return 0
        return amount_out
    except Exception as e:
        logger.error(f"Balancer simulation failed: {e}")
        return 0

# ============================================================
# UNISWAP V2
# ============================================================

def simulate_v2(reserve0: int, reserve1: int, token0: str, token_in: str, amount_in: int) -> int:
    is_token0 = token_in.lower() == token0.lower()
    reserve_in = reserve0 if is_token0 else reserve1
    reserve_out = reserve1 if is_token0 else reserve0
    if reserve_in == 0 or reserve_out == 0:
        logger.debug("V2 pool has zero liquidity")
        return 0
    amount_in_with_fee = amount_in * 997
    numerator = amount_in_with_fee * reserve_out
    denominator = (reserve_in * 1000) + amount_in_with_fee
    return numerator // denominator

# ============================================================
# GAS PRICE TRACKING
# ============================================================

class GasTracker:
    def __init__(self):
        self.last_gas_price = 0
        self.last_update = 0
        self.update_interval = 10

    async def get_gas_price(self) -> int:
        now = time.time()
        if now - self.last_update > self.update_interval:
            try:
                self.last_gas_price = await get_w3().eth.gas_price
                self.last_update = now
            except Exception as e:
                logger.error(f"Failed to get gas price: {e}")
                if self.last_gas_price == 0:
                    self.last_gas_price = 50 * 10**9
        return self.last_gas_price

    async def calculate_cost_usd(self) -> float:
        gas_price_wei = await self.get_gas_price()
        cost_matic = (ESTIMATED_GAS_USAGE * gas_price_wei) / 10**18
        cost_usd = cost_matic * MATIC_PRICE_USD
        return cost_usd

gas_tracker = GasTracker()

# ============================================================
# TOKENS
# ============================================================

USDC = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
WETH = "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619"
DAI  = "0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063"

# ============================================================
# POOLS (păstrezi structura ta, eventual ajustezi adresele)
# ============================================================

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

# ============================================================
# ROUTES (ca în codul tău)
# ============================================================

ROUTES = [
    {
        "name": "Route 1: V3(0.05%) → Balancer Weighted",
        "token_in": USDC,
        "token_mid": WETH,
        "amount_in": 1000 * 10**6,
        "min_profit": MIN_PROFIT_MODERATE,
        "step1_pool": "UNI_V3_USDC_WETH_005",
        "step2_pool": "BAL_USDC_WETH_WEIGHTED",
        "enabled": True
    },
    {
        "name": "Route 2: V3(0.3%) → V2 SushiSwap",
        "token_in": USDC,
        "token_mid": WETH,
        "amount_in": 1500 * 10**6,
        "min_profit": MIN_PROFIT_MODERATE,
        "step1_pool": "UNI_V3_USDC_WETH_03",
        "step2_pool": "SUSHI_V2_USDC_WETH",
        "enabled": True
    },
    {
        "name": "Route 3: V3 USDC/DAI → Balancer Stable",
        "token_in": USDC,
        "token_mid": DAI,
        "amount_in": 2000 * 10**6,
        "min_profit": MIN_PROFIT_CONSERVATIVE,
        "step1_pool": "UNI_V3_USDC_DAI_001",
        "step2_pool": "BAL_USDC_DAI_STABLE",
        "enabled": True
    },
]

# ============================================================
# SNAPSHOT LOGIC (identic cu al tău, doar folosește multicall-ul nostru)
# ============================================================

async def snapshot_route(route: Dict) -> Optional[Dict]:
    step1_pool_info = POOLS[route["step1_pool"]]
    step2_pool_info = POOLS[route["step2_pool"]]
    calls = []

    # step1
    if step1_pool_info["type"] == "v3":
        calls.extend([
            {"target": step1_pool_info["address"], "callData": SEL_SLOT0},
            {"target": step1_pool_info["address"], "callData": SEL_LIQUIDITY},
        ])
    elif step1_pool_info["type"] == "v2":
        calls.extend([
            {"target": step1_pool_info["address"], "callData": SEL_GETRES},
            {"target": step1_pool_info["address"], "callData": SEL_TOKEN0},
        ])
    elif step1_pool_info["type"] == "balancer":
        calls.append({"target": step1_pool_info["address"], "callData": SEL_GETPOOLID})

    # step2
    if step2_pool_info["type"] == "v3":
        calls.extend([
            {"target": step2_pool_info["address"], "callData": SEL_SLOT0},
            {"target": step2_pool_info["address"], "callData": SEL_LIQUIDITY},
        ])
    elif step2_pool_info["type"] == "v2":
        calls.extend([
            {"target": step2_pool_info["address"], "callData": SEL_GETRES},
            {"target": step2_pool_info["address"], "callData": SEL_TOKEN0},
        ])
    elif step2_pool_info["type"] == "balancer":
        calls.append({"target": step2_pool_info["address"], "callData": SEL_GETPOOLID})

    try:
        result = await multicall(calls)
    except Exception as e:
        logger.error(f"[{route['name']}] Snapshot multicall failed: {e}")
        return None

    snapshot = {"step1": {}, "step2": {}}
    idx = 0

    # step1 parse
    if step1_pool_info["type"] == "v3":
        if not result[idx][0] or not result[idx+1][0]:
            logger.error(f"[{route['name']}] V3 step1 call failed")
            return None
        sqrtP, tick, *_ = decode(
            ["uint160", "int24", "uint16", "uint16", "uint16", "uint8", "bool"],
            result[idx][1]
        )
        (liquidity,) = decode(["uint128"], result[idx+1][1])
        snapshot["step1"] = {
            "type": "v3",
            "sqrtPriceX96": sqrtP,
            "tick": tick,
            "liquidity": liquidity,
            "fee": step1_pool_info["fee"]
        }
        idx += 2
    elif step1_pool_info["type"] == "v2":
        if not result[idx][0] or not result[idx+1][0]:
            logger.error(f"[{route['name']}] V2 step1 call failed")
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
    elif step1_pool_info["type"] == "balancer":
        if not result[idx][0]:
            logger.error(f"[{route['name']}] Balancer step1 call failed")
            return None
        (pool_id,) = decode(["bytes32"], result[idx][1])
        snapshot["step1"] = {
            "type": "balancer",
            "poolId": pool_id
        }
        idx += 1

    # step2 parse
    if step2_pool_info["type"] == "v3":
        if not result[idx][0] or not result[idx+1][0]:
            logger.error(f"[{route['name']}] V3 step2 call failed")
            return None
        sqrtP, tick, *_ = decode(
            ["uint160", "int24", "uint16", "uint16", "uint16", "uint8", "bool"],
            result[idx][1]
        )
        (liquidity,) = decode(["uint128"], result[idx+1][1])
        snapshot["step2"] = {
            "type": "v3",
            "sqrtPriceX96": sqrtP,
            "tick": tick,
            "liquidity": liquidity,
            "fee": step2_pool_info["fee"]
        }
        idx += 2
    elif step2_pool_info["type"] == "v2":
        if not result[idx][0] or not result[idx+1][0]:
            logger.error(f"[{route['name']}] V2 step2 call failed")
            return None
        r0, r1, _ = decode(["uint112", "uint112", "uint32"], result[idx][1])
        (t0,) = decode(["address"], result[idx+1][1])
        snapshot["step2"] = {
            "type": "v2",
            "reserve0": r0,
            "reserve1": r1,
            "token0": t0
        }
        idx += 2
    elif step2_pool_info["type"] == "balancer":
        if not result[idx][0]:
            logger.error(f"[{route['name']}] Balancer step2 call failed")
            return None
        (pool_id,) = decode(["bytes32"], result[idx][1])
        snapshot["step2"] = {
            "type": "balancer",
            "poolId": pool_id
        }
        idx += 1

    return snapshot

# ============================================================
# SIMULATION LOGIC
# ============================================================

async def simulate_route(route: Dict, snapshot: Dict) -> Optional[int]:
    step1 = snapshot["step1"]
    if step1["type"] == "v3":
        amount_mid = await simulate_v3_accurate(
            route["token_in"], route["token_mid"], route["amount_in"], step1["fee"]
        )
    elif step1["type"] == "v2":
        amount_mid = simulate_v2(
            step1["reserve0"], step1["reserve1"], step1["token0"],
            route["token_in"], route["amount_in"]
        )
    elif step1["type"] == "balancer":
        amount_mid = await simulate_balancer(
            step1["poolId"], route["token_in"], route["token_mid"], route["amount_in"]
        )
    else:
        return None

    if amount_mid == 0:
        return None

    step2 = snapshot["step2"]
    if step2["type"] == "v3":
        final_amount = await simulate_v3_accurate(
            route["token_mid"], route["token_in"], amount_mid, step2["fee"]
        )
    elif step2["type"] == "v2":
        final_amount = simulate_v2(
            step2["reserve0"], step2["reserve1"], step2["token0"],
            route["token_mid"], amount_mid
        )
    elif step2["type"] == "balancer":
        final_amount = await simulate_balancer(
            step2["poolId"], route["token_mid"], route["token_in"], amount_mid
        )
    else:
        return None

    return final_amount

# ============================================================
# ROUTE WORKER
# ============================================================

async def route_worker(route: Dict):
    if not route.get("enabled", True):
        logger.info(f"[{route['name']}] Route disabled, skipping...")
        return

    logger.info(f"[{route['name']}] Starting worker...")
    last_tick = None
    consecutive_errors = 0
    loop_counter = 0

    while True:
        try:
            await web3_manager.heartbeat()

            snapshot = await snapshot_route(route)
            if snapshot is None:
                consecutive_errors += 1
                if consecutive_errors >= MAX_RETRIES:
                    logger.error(f"[{route['name']}] Too many consecutive errors, backing off...")
                    await asyncio.sleep(5)
                    consecutive_errors = 0
                else:
                    await asyncio.sleep(RETRY_BACKOFF)
                continue

            consecutive_errors = 0
            current_tick = snapshot["step1"].get("tick", 0)

            if current_tick == last_tick and current_tick != 0:
                await asyncio.sleep(CHECK_INTERVAL + random.uniform(0, 0.02))
                continue

            last_tick = current_tick
            final_amount = await simulate_route(route, snapshot)
            if final_amount is None:
                await asyncio.sleep(CHECK_INTERVAL + random.uniform(0, 0.02))
                continue

            gross_profit = final_amount - route["amount_in"]
            gross_profit_usdc = gross_profit / 10**6

            gas_cost_usd = await gas_tracker.calculate_cost_usd()
            net_profit = gross_profit - int(gas_cost_usd * 10**6)
            net_profit_usdc = net_profit / 10**6

            if net_profit >= route["min_profit"]:
                logger.info(
                    f"[{route['name']}] 🔥 OPPORTUNITY! "
                    f"Gross: ${gross_profit_usdc:.2f} | "
                    f"Gas: ${gas_cost_usd:.2f} | "
                    f"Net: ${net_profit_usdc:.2f} | "
                    f"Tick: {current_tick}"
                )
                msg = (
                    f"🔥 <b>OPORTUNITATE REALĂ</b>\n"
                    f"Route: {route['name']}\n"
                    f"Gross: ${gross_profit_usdc:.2f}\n"
                    f"Gas: ${gas_cost_usd:.2f}\n"
                    f"Net: ${net_profit_usdc:.2f}\n"
                    f"Tick: {current_tick}"
                )
                await telegram_send(msg)
                log_opportunity(
                    route["name"], current_tick,
                    gross_profit, net_profit, gas_cost_usd, route["amount_in"]
                )
            else:
                if loop_counter % 50 == 0:
                    logger.debug(
                        f"[{route['name']}] Net: ${net_profit_usdc:.2f} | Tick: {current_tick}"
                    )

            loop_counter += 1
            await asyncio.sleep(CHECK_INTERVAL + random.uniform(0, 0.02))

        except Exception as e:
            consecutive_errors += 1
            logger.error(f"[{route['name']}] Worker error: {e}")
            await telegram_send(f"⚠️ Worker error în <b>{route['name']}</b>:\n{e}")
            if consecutive_errors >= MAX_RETRIES:
                logger.error(f"[{route['name']}] Too many errors, backing off...")
                await asyncio.sleep(5)
                consecutive_errors = 0
            else:
                await asyncio.sleep(RETRY_BACKOFF)

# ============================================================
# MAIN
# ============================================================

async def main():
    logger.info("=" * 60)
    logger.info("Arbitrage Opportunity Monitor - Server Mode")
    logger.info("=" * 60)

    try:
        await web3_manager.init()
    except Exception as e:
        logger.error(f"❌ Failed to initialize Web3: {e}")
        return

    await telegram_send("🚀 Bot pornit pe server.\nMonitorizare oportunități activată.")

    tasks = []
    for route in ROUTES:
        if route.get("enabled", True):
            tasks.append(asyncio.create_task(route_worker(route)))

    logger.info(f"Started {len(tasks)} workers")
    logger.info("Monitoring for opportunities... Press Ctrl+C to stop")
    logger.info("=" * 60)

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
        await telegram_send("🛑 Bot oprit manual. Salvare statistici...")

        cursor = db.execute('''
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN net_profit > 0 THEN 1 ELSE 0 END) as profitable,
                AVG(net_profit) / 1000000.0 as avg_profit,
                MAX(net_profit) / 1000000.0 as max_profit
            FROM opportunities
        ''')
        stats = cursor.fetchone()
        logger.info(f"Total opportunities found: {stats[0]}")
        logger.info(f"Profitable opportunities: {stats[1]}")
        if stats[0] > 0:
            logger.info(f"Average net profit: ${stats[2]:.2f}")
            logger.info(f"Max net profit: ${stats[3]:.2f}")
        db.close()

if __name__ == "__main__":
    asyncio.run(main())
