import os
import json
import logging
import asyncio
import aiosqlite
import aiohttp
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

try:
    with open('config.json', 'r') as f:
        CONFIG = json.load(f)
except FileNotFoundError:
    logging.critical("config.json not found. Execution halted.")
    exit(1)

def format_price(val):
    """Truncates decimal float values dynamically for clean UI representation."""
    if val >= 1:
        return f"{val:,.2f}"
    return f"{val:,.6f}"

async def process_queue():
    logging.info("Executing Headless Discord Signal Bridge..")
    
    env_key = CONFIG['bridge']['discord_webhook_env_key']
    webhook_url = os.getenv(env_key)
    
    if not webhook_url:
        logging.error(f"Missing environment variable: {env_key}")
        return

    client_id = CONFIG.get('client_id', 'Unknown Client')
    strict_mode = CONFIG['consensus_engine'].get('strict_mode', True)

    async with aiosqlite.connect('signals.db', timeout=10.0) as db:
        async with db.execute("SELECT id, symbol, direction, price, stop_loss, take_profit_1, take_profit_2, indicators_triggered FROM alerts WHERE is_sent = 0") as cursor:
            unsent_alerts = await cursor.fetchall()
            
        if not unsent_alerts:
            return

        async with aiohttp.ClientSession() as session:
            for alert in unsent_alerts:
                alert_id, symbol, direction, price, stop_loss, tp1, tp2, indicators = alert
                
                # Theme alignment based on signal intent
                if direction == "LONG":
                    color_hex = CONFIG['bridge']['embed_color_bullish']
                    title = f"🟢 STRICT CONSENSUS MET: {symbol.upper().replace('_', '/')}"
                    dir_text = "📈 LONG (Bullish)"
                else:
                    color_hex = CONFIG['bridge']['embed_color_bearish']
                    title = f"🔴 STRICT CONSENSUS MET: {symbol.upper().replace('_', '/')}"
                    dir_text = "📉 SHORT (Bearish)"
                
                decimal_color = int(color_hex, 16)
                
                # Payload construction matching exact structural specifications
                payload = {
                    "embeds": [
                        {
                            "title": title,
                            "color": decimal_color,
                            "fields": [
                                {
                                    "name": "Direction",
                                    "value": dir_text,
                                    "inline": True
                                },
                                {
                                    "name": "Execution Price",
                                    "value": f"${format_price(price)}",
                                    "inline": True
                                },
                                {
                                    "name": "🎯 Trade Parameters (ATR-Adjusted)",
                                    "value": f"**Take Profit 2:** ${format_price(tp2)}\n**Take Profit 1:** ${format_price(tp1)}\n**Stop Loss:** ${format_price(stop_loss)}"
                                },
                                {
                                    "name": "📊 Indicator Matrix Alignment",
                                    "value": indicators
                                }
                            ],
                            "footer": {
                                "text": f"Client ID: {client_id} | Strict Mode: {strict_mode}"
                            }
                        }
                    ]
                }
                
                # Network transmission with back-off handling wrapper
                async with session.post(webhook_url, json=payload) as response:
                    if response.status in [200, 204]:
                        await db.execute("UPDATE alerts SET is_sent = 1 WHERE id = ?", (alert_id,))
                        await db.commit()
                        logging.info(f"Dispatched webhook for {symbol} ({direction})")
                    elif response.status == 429:
                        logging.warning("Discord Rate Limit Hit. Queuing remainder for next cycle.")
                        break
                    else:
                        logging.error(f"Webhook Delivery Failed HTTP {response.status}")

if __name__ == "__main__":
    asyncio.run(process_queue())
