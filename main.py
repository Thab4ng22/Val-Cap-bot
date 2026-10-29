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

app = FastAPI(title="Local MT5 Multi-Account Orchestrator")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

TELEGRAM_BOT_TOKEN = "YOUR_TELEGRAM_BOT_TOKEN"
TELEGRAM_CHAT_ID = "YOUR_TELEGRAM_CHAT_ID"

# Database storing accounts connected via the HTML page
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

@app.post("/api/accounts/add")
def add_account(acc: AccountRegistration):
    """Dynamically logs into an MT5 account when submitted from the HTML UI."""
    if not mt5.initialize():
        return {"status": "ERROR", "message": "MT5 terminal failed to initialize."}
    
    authorized = mt5.login(login=acc.login, password=acc.password, server=acc.server)
    if authorized:
        ACCOUNTS_DB[acc.login] = acc.dict()
        logging.info(f"Successfully connected account #{acc.login} on server {acc.server}")
        return {"status": "SUCCESS", "message": f"Account #{acc.login} connected successfully!"}
    else:
        err = mt5.last_error()
        return {"status": "ERROR", "message": f"Authentication failed. MT5 Error: {err}"}

@app.get("/api/accounts/list")
def list_accounts():
    """Returns telemetry (balance, equity, profit) for all connected accounts."""
    telemetry = []
    for login, creds in ACCOUNTS_DB.items():
        if mt5.login(login=creds["login"], password=creds["password"], server=creds["server"]):
            acc_info = mt5.account_info()
            if acc_info:
                telemetry.append({
                    "login": creds["login"],
                    "server": creds["server"],
                    "balance": acc_info.balance,
                    "equity": acc_info.equity,
                    "profit": acc_info.profit,
                    "active_pairs": creds["active_pairs"]
                })
    return telemetry

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_connections.append(websocket)
    try:
        while True:
            market_data = {}
            if ACCOUNTS_DB:
                # Use the first active account session to fetch live market chart data
                first_acc = list(ACCOUNTS_DB.values())[0]
                if mt5.login(login=first_acc["login"], password=first_acc["password"], server=first_acc["server"]):
                    rates = mt5.copy_rates_from_pos("XAUUSD", mt5.TIMEFRAME_M15, 0, 60)
                    if rates is not None and len(rates) >= 20:
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

                        market_data = {
                            "type": "MARKET_UPDATE",
                            "symbol": "XAUUSD",
                            "currentPrice": close,
                            "resistance": res,
                            "support": supp,
                            "signal": signal,
                            "score": score,
                            "candles": candles
                        }

            if market_data:
                await websocket.send_text(json.dumps(market_data))
            await asyncio.sleep(1.5)
    except WebSocketDisconnect:
        active_connections.remove(websocket)

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
