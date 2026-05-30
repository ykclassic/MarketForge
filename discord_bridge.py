import os
import json
import asyncio
import logging
from datetime import datetime, timezone
import aiohttp
import aiosqlite
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

try:
    with open('config.json', 'r') as f:
        CONFIG = json.load(f)
except FileNotFoundError:
    logging.critical("config.json not found. Execution halted.")
    exit(1)

env_key = CONFIG['bridge']['discord_webhook_env_key']
DISCORD_WEBHOOK_URL = os.getenv(env_key)

def build_discord_embed(symbol: str, direction: str, indicators: str, timestamp_ms: int) -> dict:
    try:
        color_hex = CONFIG['bridge']['embed_color_bullish'] if direction == "BULLISH" else CONFIG['bridge']['embed_color_bearish']
        color = int(color_hex, 16)
    except ValueError:
        color = 0x00FF00 if direction == "BULLISH" else 0xFF0000

    emoji = "🟢" if direction == "BULLISH" else "🔴"
    dt_utc = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
    formatted_time = dt_utc.strftime('%Y-%m-%d %H:%M:%S UTC')

    embed = {
        "title": f"{emoji} RICH SIGNAL ALERT: {symbol.upper()}",
        "description": "High-probability multi-timeframe consensus achieved.",
        "color": color,
        "fields": [
            {"name": "Asset Ticker", "value": f"**{symbol.upper()}**", "inline": True},
            {"name": "Market Bias", "value": f"**{direction}**", "inline": True},
            {"name": "Indicators Triggered", "value": f"`{indicators}`", "inline": False},
            {"name": "Risk Protocol", "value": "Strict invalidation required at local swing high/low.", "inline": False}
        ],
        "footer": {"text": f"Algorithmic Execution Engine | {formatted_time}"}
    }
    return {"embeds": [embed]}

async def dispatch_webhook(session: aiohttp.ClientSession, payload: dict) -> bool:
    if not DISCORD_WEBHOOK_URL:
        logging.error(f"Webhook URL missing. Ensure {env_key} is set.")
        return False

    headers = {"Content-Type": "application/json"}
    
    try:
        async with session.post(DISCORD_WEBHOOK_URL, data=json.dumps(payload), headers=headers) as response:
            if response.status in (200, 204):
                return True
            elif response.status == 429:
                retry_after = (await response.json()).get("retry_after", 1)
                logging.warning(f"Rate limited by Discord. Retrying after {retry_after}s...")
                await asyncio.sleep(retry_after)
                return await dispatch_webhook(session, payload)
            else:
                logging.error(f"Discord Error {response.status}: {await response.text()}")
                return False
    except aiohttp.ClientError as e:
        logging.error(f"Network error during webhook dispatch: {e}")
        return False

async def process_signal_queue():    
    async with aiohttp.ClientSession() as session:
        while True:
            async with aiosqlite.connect('signals.db', timeout=10.0) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("SELECT * FROM alerts WHERE is_sent = 0 ORDER BY timestamp ASC") as cursor:
                    unsent_alerts = await cursor.fetchall()
                
                if unsent_alerts:
                    for alert in unsent_alerts:
                        payload = build_discord_embed(
                            symbol=alert["symbol"],
                            direction=alert["direction"],
                            indicators=alert["indicators_triggered"],
                            timestamp_ms=alert["timestamp"]
                        )
                        
                        success = await dispatch_webhook(session, payload)
                        
                        if success:
                            await db.execute("UPDATE alerts SET is_sent = 1 WHERE id = ?", (alert["id"],))
                            await db.commit()
                            logging.info(f"Signal {alert['id']} bridged to Discord.")
                        
                        await asyncio.sleep(0.5) 
            
            # Note: In a CI/CD cron environment, an infinite loop blocks the runner.
            # To execute completely via GitHub Actions, the while loop will exit if no signals remain.
            break

if __name__ == "__main__":
    logging.info("Executing Headless Discord Signal Bridge...")
    asyncio.run(process_signal_queue())
