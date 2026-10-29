import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, List
import MetaTrader5 as mt5
import requests
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

app = FastAPI(title="MT5 Multi-Account Breakout Engine")

# Enable CORS for GitHub Pages / Local Frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration & State Storage
TELEGRAM_BOT_TOKEN = "YOUR_TELEGRAM_BOT_TOKEN"
TELEGRAM_CHAT_ID = "YOUR_TELEGRAM_CHAT_ID"

ACCOUNTS_DB: Dict[int, dict] = {}
active_connections: List[WebSocket] = []

class AccountRegistration(BaseModel):
    login: int
    password: str
    server: str
    active_pairs: List[str] = ["XAUUSD"]

def send_telegram_alert(msg: str):
    if TELEGRAM_BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN":
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"}
    try:
        requests.post(url, json=payload, timeout=3)
    except Exception as e:
        logging.error(f"Telegram error: {e}")

def switch_account_session(login: int, password: str, server: str) -> bool:
    if not mt5.initialize():
        return False
    return mt5.login(login=login, password=password, server=server)

def get_market_telemetry(symbol: str = "XAUUSD"):
    rates = mt5.copy_rates_from_pos(symbol, mt5.TIMEFRAME_M15, 0, 60)
    if rates is None or len(rates) < 20:
        return None

    candles = [{
        "time": datetime.fromtimestamp(r['time']).isoformat(),
        "open": float(r['open']),
        "high": float(r['high']),
        "low": float(r['low']),
        "close": float(r['close']),
        "volume": int(r['tick_volume'])
    } for r in rates]

    latest = rates[-1]
    close = float(latest['close'])
    lookback = rates[-35:-5]
    res = max(r['high'] for r in lookback)
    supp = min(r['low'] for r in lookback)
    sma_vol = sum(r['tick_volume'] for r in rates[-20:]) / 20.0

    signal = "HOLD"
    if close > res: signal = "BUY"
    elif close < supp: signal = "SELL"

    score = 0
    if signal != "HOLD":
        vol_score = 35 if (latest['tick_volume'] / sma_vol) >= 1.5 else 18
        score = vol_score + 25 + 25 + 15

    return {
        "symbol": symbol,
        "currentPrice": close,
        "resistance": res,
        "support": supp,
        "signal": signal,
        "score": score,
        "candles": candles
    }

@app.post("/api/accounts/add")
def add_account(acc: AccountRegistration):
    if switch_account_session(acc.login, acc.password, acc.server):
        ACCOUNTS_DB[acc.login] = acc.dict()
        return {"status": "SUCCESS", "message": f"Account #{acc.login} connected."}
    return {"status": "ERROR", "message": "Authentication failed."}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_connections.append(websocket)
    try:
        while True:
            if switch_account_session(list(ACCOUNTS_DB.values())[0]["login"], list(ACCOUNTS_DB.values())[0]["password"], list(ACCOUNTS_DB.values())[0]["server"]) if ACCOUNTS_DB else mt5.initialize():
                data = get_market_telemetry("XAUUSD")
                if data:
                    data["type"] = "MARKET_UPDATE"
                    await websocket.send_text(json.dumps(data))
            await asyncio.sleep(1.5)
    except WebSocketDisconnect:
        active_connections.remove(websocket)

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
