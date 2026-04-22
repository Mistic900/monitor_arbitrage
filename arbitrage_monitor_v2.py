import asyncio
import os
import sqlite3
import logging
import time
import random
from typing import Dict, List, Optional

import httpx
from web3 import AsyncWeb3, Web3
from eth_abi import decode
from eth_utils import keccak
from logging.handlers import RotatingFileHandler

# ============================================================
# CONFIG
# ============================================================

RPC_HTTP_FALLBACK = "https://polygon-rpc.com"

CHECK_INTERVAL = 0.05
MAX_RETRIES = 3
RETRY_BACKOFF = 0.5

MIN_PROFIT_CONSERVATIVE = 10 * 10**6   # 10 USDC
MIN_PROFIT_MODERATE = 7 * 10**6        # 7 USDC
MIN_PROFIT_AGGRESSIVE = 5 * 10**6      # 5 USDC

ESTIMATED_GAS_USAGE = 350_000
MATIC_PRICE_USD = 0.80

ENABLE_TELEGRAM = True
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = int(os.getenv("TELEGRAM_CHAT_ID", "0") or "0")

# ============================================================
# LOGGING
# ============================================================

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
# DB
# ============================================================

db = sqlite3.connect('arbitrage_opportunities.db', check_same_thread=False)
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
# TELEGRAM
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
# WEB3 MANAGER – HTTP only, cu fallback
# ============================================================


def _wss_to_https(url: str) -> str:
    if url.startswith("wss://"):
        return "https://" + url[len("wss://"):]
    return url


def _build_rpc_endpoints() -> List[Dict]:
    env_vars = [
        ("RPC_WSS_PRIMARY", "primary"),
        ("RPC_WSS_BACKUP", "backup"),
        ("ALCHEMY_WSS", "alchemy"),
        ("POLYGON_PUBLIC", "polygon_public"),
        ("MATIC_PUBLIC", "matic_public"),
    ]
    endpoints = []
    for env_name, label in env_vars:
        raw = os.getenv(env_name, "").strip()
        if raw:
            url = _wss_to_https(raw)
            endpoints.append({"label": label, "url": url})
            logger.info(f"RPC endpoint loaded from {env_name}: {url[:60]}...")
        else:
            logger.warning(f"Environment variable {env_name} is not set — skipping.")

    endpoints.append({"label": "fallback", "url": RPC_HTTP_FALLBACK})
    return endpoints


class Web3Manager:
    def __init__(self):
        self.w3: Optional[AsyncWeb3] = None
        self.last_heartbeat = 0
        self.heartbeat_interval = 10
        self.lock = asyncio.Lock()
        self.rpc_endpoints: List[Dict] = _build_rpc_endpoints()
        self.current_index: int = 0
        self.current_rpc: str = (
            self.rpc_endpoints[0]["label"] if self.rpc_endpoints else "fallback"
        )

    async def init(self):
        await self._connect(0)

    async def _connect(self, index: int):
        if not self.rpc_endpoints:
            raise RuntimeError("No RPC endpoints configured.")
        index = index % len(self.rpc_endpoints)
        endpoint = self.rpc_endpoints[index]
        label = endpoint["label"]
        url = endpoint["url"]
        logger.info(f"Connecting to RPC '{label}': {url[:60]}...")
        self.w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(url))
        self.current_index = index
        self.current_rpc = label

        try:
            block = await self.w3.eth.block_number
            logger.info(f"✅ Connected via '{label}', block={block}")
            await telegram_send(f"✅ RPC conectat: <b>{label}</b>, block={block}")
        except Exception as e:
            logger.error(f"RPC connect failed ('{label}'): {e}")
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
                logger.error(f"Heartbeat failed on '{self.current_rpc}': {e}")
                await telegram_send(f"⚠️ Heartbeat failed pe <b>{self.current_rpc}</b>: {e}")
                await self._failover()

    async def _failover(self):
        next_index = (self.current_index + 1) % len(self.rpc_endpoints)
        next_label = self.rpc_endpoints[next_index]["label"]
        try:
            await self._connect(next_index)
            await telegram_send(f"🔄 RPC failover → <b>{self.current_rpc}</b>")
        except Exception as e:
            logger.error(f"Failover to '{next_label}' failed: {e}")
            fallback_index = len(self.rpc_endpoints) - 1
            if next_index != fallback_index:
                try:
                    await self._connect(fallback_index)
                except Exception as e2:
                    logger.error(f"Fallback connect also failed: {e2}")


web3_manager = Web3Manager()


def get_w3() -> AsyncWeb3:
    if web3_manager.w3 is None:
        raise RuntimeError("Web3 not initialized")
    return web3_manager.w3


client = httpx.AsyncClient(timeout=10.0)

# ============================================================
# HELPERS & SELECTORS
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


async def multicall(calls: List[Dict], require_success: bool = False) -> List:
    """
    calls: [{"target": checksum_address, "callData": bytes}, ...]
    return: [(success: bool, returnData: bytes), ...]
    """
    contract = get_w3().eth.contract(address=MULTICALL, abi=MULTICALL_ABI)

    formatted_calls = [
        (call["target"], call["callData"])
        for call in calls
    ]

    for attempt in range(MAX_RETRIES):
        try:
            result = await contract.functions.tryAggregate(
                require_success,
                formatted_calls
            ).call()
            return result
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                logger.error(f"Multicall failed after {MAX_RETRIES} attempts: {e}")
                raise
            logger.warning(f"Multicall attempt {attempt + 1} failed: {e}")
            await asyncio.sleep(RETRY_BACKOFF * (attempt + 1))

# ============================================================
# QUOTER V3
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
        # result este int256[] cu lungime = len(assets)
        # pentru 2 assets, index 1 este out amount
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
# GAS
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
# TOKENS & POOLS & ROUTES
# ============================================================

USDC = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
WETH = "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619"
DAI = "0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063"

POOLS: Dict[str, Dict] = {
    "UNI_V3_USDC_WETH_005": {
        "address": "0x45dDa9cb7c25131DF268515131f647d726f50608",
        "type": "v3",
        "fee": 500
    },
    "UNI_V3_USDC_WETH_03": {
        "address": "0xA374d23C3bA9A0b3e3D3b5f3C2f0D0Ce1F0a5A",
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

# convertim toate adresele în checksum
for pool in POOLS.values():
    pool["address"] = Web3.to_checksum_address(pool["address"])

ROUTES: List[Dict] = [
    {
        "name": "R1: UNI V3 0.05% → UNI V3 0.3% (USDC/WETH/USDC)",
        "token_in": USDC,
        "token_mid": WETH,
        "amount_in": 2500 * 10**6,
        "min_profit": 10 * 10**6,
        "step1_pool": "UNI_V3_USDC_WETH_005",
        "step2_pool": "UNI_V3_USDC_WETH_03",
        "enabled": True
    },
    {
        "name": "R2: UNI V3 0.3% → BAL Weighted (USDC/WETH/USDC)",
        "token_in": USDC,
        "token_mid": WETH,
        "amount_in": 3000 * 10**6,
        "min_profit": 12 * 10**6,
        "step1_pool": "UNI_V3_USDC_WETH_03",
        "step2_pool": "BAL_USDC_WETH_WEIGHTED",
        "enabled": True
    },
    {
        "name": "R3: UNI V3 0.05% → SUSHI V2 (USDC/WETH/USDC)",
        "token_in": USDC,
        "token_mid": WETH,
        "amount_in": 2000 * 10**6,
        "min_profit": 8 * 10**6,
        "step1_pool": "UNI_V3_USDC_WETH_005",
        "step2_pool": "SUSHI_V2_USDC_WETH",
        "enabled": True
    },
    {
        "name": "R4: UNI V3 0.01% → BAL Stable (USDC/DAI/USDC)",
        "token_in": USDC,
        "token_mid": DAI,
        "amount_in": 5000 * 10**6,
        "min_profit": 15 * 10**6,
        "step1_pool": "UNI_V3_USDC_DAI_001",
        "step2_pool": "BAL_USDC_DAI_STABLE",
        "enabled": True
    },
    {
        "name": "R5: BAL Stable → UNI V3 0.01% (USDC/DAI/USDC)",
        "token_in": USDC,
        "token_mid": DAI,
        "amount_in": 4000 * 10**6,
        "min_profit": 12 * 10**6,
        "step1_pool": "BAL_USDC_DAI_STABLE",
        "step2_pool": "UNI_V3_USDC_DAI_001",
        "enabled": True
    }
]

# ============================================================
# GLOBAL SNAPSHOT CACHE (multicall batching)
# ============================================================

ROUTE_SNAPSHOTS: Dict[str, Optional[Dict]] = {}
SNAPSHOT_LOCK = asyncio.Lock()

# ============================================================
# SNAPSHOT BUILD & PARSE
# ============================================================


def _to_bytes(selector_hex: str) -> bytes:
    return bytes.fromhex(selector_hex[2:])


def build_calls_for_route(route: Dict) -> List[Dict]:
    step1_pool_info = POOLS[route["step1_pool"]]
    step2_pool_info = POOLS[route["step2_pool"]]
    calls: List[Dict] = []

    # step1
    if step1_pool_info["type"] == "v3":
        calls.extend([
            {"target": step1_pool_info["address"], "callData": _to_bytes(SEL_SLOT0)},
            {"target": step1_pool_info["address"], "callData": _to_bytes(SEL_LIQUIDITY)},
        ])
    elif step1_pool_info["type"] == "v2":
        calls.extend([
            {"target": step1_pool_info["address"], "callData": _to_bytes(SEL_GETRES)},
            {"target": step1_pool_info["address"], "callData": _to_bytes(SEL_TOKEN0)},
        ])
    elif step1_pool_info["type"] == "balancer":
        calls.append({"target": step1_pool_info["address"], "callData": _to_bytes(SEL_GETPOOLID)})

    # step2
    if step2_pool_info["type"] == "v3":
        calls.extend([
            {"target": step2_pool_info["address"], "callData": _to_bytes(SEL_SLOT0)},
            {"target": step2_pool_info["address"], "callData": _to_bytes(SEL_LIQUIDITY)},
        ])
    elif step2_pool_info["type"] == "v2":
        calls.extend([
            {"target": step2_pool_info["address"], "callData": _to_bytes(SEL_GETRES)},
            {"target": step2_pool_info["address"], "callData": _to_bytes(SEL_TOKEN0)},
        ])
    elif step2_pool_info["type"] == "balancer":
        calls.append({"target": step2_pool_info["address"], "callData": _to_bytes(SEL_GETPOOLID)})

    return calls


def parse_snapshot_for_route(route: Dict, result_slice: List) -> Optional[Dict]:
    step1_pool_info = POOLS[route["step1_pool"]]
    step2_pool_info = POOLS[route["step2_pool"]]
    snapshot: Dict[str, Dict] = {"step1": {}, "step2": {}}
    idx = 0

    # step1
    if step1_pool_info["type"] == "v3":
        if not result_slice[idx][0] or not result_slice[idx + 1][0]:
            return None
        sqrtP, tick, *_ = decode(
            ["uint160", "int24", "uint16", "uint16", "uint16", "uint8", "bool"],
            result_slice[idx][1]
        )
        (liquidity,) = decode(["uint128"], result_slice[idx + 1][1])
        snapshot["step1"] = {
            "type": "v3",
            "sqrtPriceX96": sqrtP,
            "tick": tick,
            "liquidity": liquidity,
            "fee": step1_pool_info["fee"]
        }
        idx += 2
    elif step1_pool_info["type"] == "v2":
        if not result_slice[idx][0] or not result_slice[idx + 1][0]:
            return None
        r0, r1, _ = decode(["uint112", "uint112", "uint32"], result_slice[idx][1])
        (t0,) = decode(["address"], result_slice[idx + 1][1])
        snapshot["step1"] = {
            "type": "v2",
            "reserve0": r0,
            "reserve1": r1,
            "token0": t0
        }
        idx += 2
    elif step1_pool_info["type"] == "balancer":
        if not result_slice[idx][0]:
            return None
        (pool_id,) = decode(["bytes32"], result_slice[idx][1])
        snapshot["step1"] = {
            "type": "balancer",
            "poolId": pool_id
        }
        idx += 1

    # step2
    if step2_pool_info["type"] == "v3":
        if not result_slice[idx][0] or not result_slice[idx + 1][0]:
            return None
        sqrtP, tick, *_ = decode(
            ["uint160", "int24", "uint16", "uint16", "uint16", "uint8", "bool"],
            result_slice[idx][1]
        )
        (liquidity,) = decode(["uint128"], result_slice[idx + 1][1])
        snapshot["step2"] = {
            "type": "v3",
            "sqrtPriceX96": sqrtP,
            "tick": tick,
            "liquidity": liquidity,
            "fee": step2_pool_info["fee"]
        }
        idx += 2
    elif step2_pool_info["type"] == "v2":
        if not result_slice[idx][0] or not result_slice[idx + 1][0]:
            return None
        r0, r1, _ = decode(["uint112", "uint112", "uint32"], result_slice[idx][1])
        (t0,) = decode(["address"], result_slice[idx + 1][1])
        snapshot["step2"] = {
            "type": "v2",
            "reserve0": r0,
            "reserve1": r1,
            "token0": t0
        }
        idx += 2
    elif step2_pool_info["type"] == "balancer":
        if not result_slice[idx][0]:
            return None
        (pool_id,) = decode(["bytes32"], result_slice[idx][1])
        snapshot["step2"] = {
            "type": "balancer",
            "poolId": pool_id
        }
        idx += 1

    return snapshot

# ============================================================
# SNAPSHOT LOOP – GLOBAL MULTICALL BATCHING
# ============================================================


async def snapshot_loop():
    global ROUTE_SNAPSHOTS
    while True:
        try:
            await web3_manager.heartbeat()

            all_calls: List[Dict] = []
            route_call_ranges: List[tuple] = []  # (route_name, start_idx, end_idx)

            for route in ROUTES:
                if not route.get("enabled", True):
                    continue
                route_calls = build_calls_for_route(route)
                start = len(all_calls)
                all_calls.extend(route_calls)
                end = len(all_calls)
                route_call_ranges.append((route["name"], start, end))

            if not all_calls:
                await asyncio.sleep(CHECK_INTERVAL)
                continue

            result = await multicall(all_calls, require_success=False)

            async with SNAPSHOT_LOCK:
                for route_name, start, end in route_call_ranges:
                    route = next(r for r in ROUTES if r["name"] == route_name)
                    slice_res = result[start:end]
                    snap = parse_snapshot_for_route(route, slice_res)
                    ROUTE_SNAPSHOTS[route_name] = snap

        except Exception as e:
            logger.error(f"[SNAPSHOT_LOOP] error: {e}")
        finally:
            await asyncio.sleep(CHECK_INTERVAL)

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
# WORKER
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
            async with SNAPSHOT_LOCK:
                snapshot = ROUTE_SNAPSHOTS.get(route["name"])

            if snapshot is None:
                await asyncio.sleep(CHECK_INTERVAL)
                continue

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
    logger.info("Arbitrage Opportunity Monitor - MEV-Core v3")
    logger.info("=" * 60)

    try:
        await web3_manager.init()
    except Exception as e:
        logger.error(f"❌ Failed to initialize Web3: {e}")
        return

    await telegram_send("🚀 Bot MEV-Core v3 pornit pe server.\nMonitorizare oportunități activată.")

    tasks = []

    # task global de snapshot batching
    tasks.append(asyncio.create_task(snapshot_loop()))

    # workers pe rute
    for route in ROUTES:
        if route.get("enabled", True):
            tasks.append(asyncio.create_task(route_worker(route)))

    logger.info(f"Started {len(tasks)} tasks (1 snapshot loop + {len(ROUTES)} workers)")
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
