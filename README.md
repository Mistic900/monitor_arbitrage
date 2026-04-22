Server Quick Start - 3 Comenzi
🎯 Ce Face?
Monitor 24/7 pentru oportunități de arbitraj pe Polygon:

✅ Monitorizează Uniswap V3, SushiSwap, Balancer
✅ Calculează profit după GAS + FEES
✅ Notificări Telegram când găsește oportunități
✅ Auto-restart dacă pică
✅ Health check endpoint
🚀 Deployment în 3 Pași
Opțiunea 1: Docker (Cel Mai Simplu)
# 1. Setup automat
./setup.sh
# Alege opțiunea 1 (Docker)
# Introdu Alchemy URL, Telegram token, etc.

# 2. Check status
docker-compose logs -f

# 3. Health check
curl http://localhost:8080/health
Opțiunea 2: Systemd (Native Linux)
# 1. Setup automat
./setup.sh
# Alege opțiunea 2 (Systemd)
# Introdu credentials

# 2. Check status
sudo systemctl status arbitrage-monitor

# 3. Logs
sudo journalctl -u arbitrage-monitor -f
📋 De Ce Ai Nevoie
1. RPC Access (OBLIGATORIU)
Gratis (limitat):

https://polygon-rpc.com
Paid (RECOMANDAT):

Alchemy: https://alchemy.com → Create app → Copy WSS URL
Infura: https://infura.io → Create project → Copy WSS URL
2. Telegram Bot (OBLIGATORIU)
1. Telegram → @BotFather → /newbot
2. Primești BOT_TOKEN
3. Start bot-ul
4. Telegram → @userinfobot → primești CHAT_ID
📊 Ce Primești
Telegram Notifications
🔥 OPPORTUNITY
Route: V3(0.05%) → Balancer Weighted
Gross: $34.56
Gas: $0.12
Net: $34.44
Tick: 204567
MATIC: $0.5234 | Gas: 45.2gwei
Health Endpoint
curl http://localhost:8080/health
Response:

{
  "status": "healthy",
  "uptime_hours": 12.5,
  "opportunities_found": 1247,
  "opportunities_profitable": 34,
  "total_net_profit_usd": 987.65,
  "matic_price": 0.5234
}
🔧 Comenzi Utile
Docker
# Logs real-time
docker-compose logs -f

# Stop
docker-compose down

# Restart
docker-compose restart

# Stats
docker stats
Systemd
# Status
sudo systemctl status arbitrage-monitor

# Logs
sudo journalctl -u arbitrage-monitor -f

# Restart
sudo systemctl restart arbitrage-monitor

# Stop
sudo systemctl stop arbitrage-monitor
⚙️ Configurare
Profit Thresholds
Editează în arbitrage_monitor_v2.py:

# În class Config:
MIN_PROFIT_CONSERVATIVE = 50 * 10**6   # $50
MIN_PROFIT_MODERATE = 20 * 10**6       # $20 ← default
MIN_PROFIT_AGGRESSIVE = 10 * 10**6     # $10
Sau în Docker (docker-compose.yml):

environment:
  - MIN_PROFIT_USD=20  # schimbă aici
Check Interval
# Mai rapid (mai mult CPU):
CHECK_INTERVAL = 0.02  # 20ms

# Default:
CHECK_INTERVAL = 0.05  # 50ms

# Mai lent (mai puțin CPU):
CHECK_INTERVAL = 0.1   # 100ms
📈 Ce Să Aștepți
După 24h Rulare (Exemplu)
Opportunities found: ~3,000-5,000
Profitable (>$20): ~50-150 (1-3%)
Total net profit: $500-2,000 (potential, nu real!)
IMPORTANT:

Script-ul NU execută tranzacții!
Doar monitorizează și notifică
Pentru execuție reală ai nevoie de smart contracts + MEV protection
Când E Profitabil?
✅ Volatilitate mare pe markets
✅ Gas price scăzut (<50 gwei)
✅ Lichiditate mare în pools
❌ Markets calme → puține oportunități
❌ Gas scump (>100 gwei) → toate unprofitable
🛠️ Troubleshooting
"All RPC endpoints failed"
# Test RPC
curl -X POST https://polygon-rpc.com \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}'

# Ar trebui să vezi: {"jsonrpc":"2.0","id":1,"result":"0x..."}
→ Dacă nu merge, RPC-ul e down. Folosește alt endpoint.

"No opportunities"
Normal! Markets sunt eficiente.

Tweaks:

Scade MIN_PROFIT_USD la 10-15
Așteaptă volatilitate
Verifică logs pentru erori
"Too many notifications"
# În Config:
TELEGRAM_BATCH_INTERVAL = 120  # batch la 2 minute (de la 60s)
📚 Full Docs
README_SERVER.md - Documentație completă
Dockerfile - Container config
docker-compose.yml - Docker orchestration
arbitrage-monitor.service - Systemd service
⚠️ Disclaimer
Tool doar pentru research/monitoring!

Pentru execuție reală ai nevoie de:

Smart contracts (flashswap)
MEV protection (Flashbots)
Infrastructure (low-latency)
Capital + gas
NU garantăm profit! Markets se mișcă rapid, competition e mare.

Succes! 🚀

Questions? Check README_SERVER.md
