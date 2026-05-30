import os
import json
import asyncio
import logging
import aiohttp
import aiosqlite
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

# Load configuration dynamically
try:
    with open('config.json', 'r') as f:
        CONFIG = json.load(f)
except FileNotFoundError:
    logging.critical("config.json not found. Execution halted.")
    exit(1)

XT_API_BASE_URL = os.getenv("XT_API_BASE_URL", CONFIG['data_ingestion']['exchange_url'])

async def init_db():
    async with aiosqlite.connect('signals.db', timeout=10.0) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS market_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                interval TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL,
                UNIQUE(symbol, interval, timestamp)
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL,
                indicators_triggered TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                is_sent INTEGER DEFAULT 0
            )
        ''')
        await db.commit()
        logging.info("Database 'signals.db' schemas verified.")

async def fetch_xt_market_data(session: aiohttp.ClientSession, symbol: str, interval: str) -> list:
    endpoint = f"{XT_API_BASE_URL}/v4/public/kline"
    params = {"symbol": symbol, "interval": interval}
    
    try:
        async with session.get(endpoint, params=params) as response:
            if response.status == 200:
                data = await response.json()
                if "result" in data and isinstance(data["result"], list):
                    return data["result"]
                else:
                    logging.error(f"Malformed response for {symbol}: {data}")
                    return []
            else:
                logging.error(f"HTTP {response.status}: Fetch failed for {symbol}.")
                return []
    except Exception as e:
        logging.error(f"Network error during data fetch: {e}")
        return []

async def ingest_and_sanitize(symbol: str, interval: str):
    async with aiohttp.ClientSession() as session:
        raw_data = await fetch_xt_market_data(session, symbol, interval)
        
        if not raw_data:
            return

        async with aiosqlite.connect('signals.db', timeout=10.0) as db:
            for candle in raw_data:
                try:
                    # Corrected XT.com mapping: [t, o, c, h, l, q, v]
                    timestamp = int(candle[0])
                    open_price = float(candle[1])
                    close_price = float(candle[2])
                    high_price = float(candle[3])
                    low_price = float(candle[4])
                    volume = float(candle[6]) # Index 6 is Volume, 5 is Quote Volume
                    
                    await db.execute('''
                        INSERT OR IGNORE INTO market_data 
                        (symbol, interval, timestamp, open, high, low, close, volume)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (symbol, interval, timestamp, open_price, high_price, low_price, close_price, volume))
                except (IndexError, ValueError, TypeError) as e:
                    logging.warning(f"Data sanitization failed: {e}")
                    continue
            
            await db.commit()
            logging.info(f"Ingested records for {symbol} ({interval}).")

async def main():
    await init_db()
    symbols = CONFIG['data_ingestion']['symbols']
    intervals = CONFIG['data_ingestion']['intervals']
    
    tasks = [ingest_and_sanitize(sym, ivl) for sym in symbols for ivl in intervals]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
