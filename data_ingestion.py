import os
import json
import asyncio
import logging
import aiohttp
import aiosqlite
from dotenv import load_dotenv

# Configure logging for production-grade observability
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
    """Initializes the database schema with automatic state migrations for new features."""
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
                price REAL DEFAULT 0.0,
                stop_loss REAL DEFAULT 0.0,
                take_profit_1 REAL DEFAULT 0.0,
                take_profit_2 REAL DEFAULT 0.0,
                indicators_triggered TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                is_sent INTEGER DEFAULT 0
            )
        ''')
        
        # Schema Migration: Add new risk columns if they don't exist in an older database
        cursor = await db.execute("PRAGMA table_info(alerts)")
        columns = [row[1] for row in await cursor.fetchall()]
        if 'price' not in columns:
            await db.execute("ALTER TABLE alerts ADD COLUMN price REAL DEFAULT 0.0")
            await db.execute("ALTER TABLE alerts ADD COLUMN stop_loss REAL DEFAULT 0.0")
            await db.execute("ALTER TABLE alerts ADD COLUMN take_profit_1 REAL DEFAULT 0.0")
            await db.execute("ALTER TABLE alerts ADD COLUMN take_profit_2 REAL DEFAULT 0.0")
            logging.info("Database schema migrated: Added ATR risk management columns.")

        await db.commit()
        logging.info("Database 'signals.db' initialized.")

async def fetch_xt_market_data(session: aiohttp.ClientSession, symbol: str, interval: str) -> list:
    """Fetches and extracts the kline record list from the XT.com v4 API envelope."""
    endpoint = f"{XT_API_BASE_URL}/v4/public/kline"
    params = {"symbol": symbol, "interval": interval}
    
    try:
        async with session.get(endpoint, params=params) as response:
            if response.status == 200:
                payload = await response.json()
                records = payload.get("result", [])
                if isinstance(records, list):
                    return records
                else:
                    logging.error(f"Unexpected response structure for {symbol}: {payload}")
                    return []
            else:
                logging.error(f"HTTP {response.status}: Fetch failed for {symbol}.")
                return []
    except Exception as e:
        logging.error(f"Network error during data fetch: {e}")
        return []

async def ingest_and_sanitize(symbol: str, interval: str):
    """Parses dictionary candle data and performs safe database insertion."""
    async with aiohttp.ClientSession() as session:
        records = await fetch_xt_market_data(session, symbol, interval)
        
        if not records:
            return

        async with aiosqlite.connect('signals.db', timeout=10.0) as db:
            for candle in records:
                if not isinstance(candle, dict) or 't' not in candle:
                    logging.warning(f"Skipping malformed candle: {candle}")
                    continue
                
                try:
                    timestamp = int(candle['t'])
                    open_price = float(candle['o'])
                    high_price = float(candle['h'])
                    low_price = float(candle['l'])
                    close_price = float(candle['c'])
                    volume = float(candle['q']) 
                    
                    await db.execute('''
                        INSERT OR IGNORE INTO market_data 
                        (symbol, interval, timestamp, open, high, low, close, volume)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (symbol, interval, timestamp, open_price, high_price, low_price, close_price, volume))
                except (ValueError, TypeError, KeyError) as e:
                    logging.warning(f"Sanitization error for {symbol}: {e} | Candle data: {candle}")
                    continue
            
            await db.commit()
            logging.info(f"Ingested {len(records)} records for {symbol} ({interval}).")

async def main():
    await init_db()
    symbols = CONFIG['data_ingestion']['symbols']
    intervals = CONFIG['data_ingestion']['intervals']
    
    tasks = [ingest_and_sanitize(sym, ivl) for sym in symbols for ivl in intervals]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
