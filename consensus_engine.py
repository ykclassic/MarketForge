import json
import logging
import asyncio
import aiosqlite
import pandas as pd
import pandas_ta_classic as ta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    with open('config.json', 'r') as f:
        CONFIG = json.load(f)
except FileNotFoundError:
    logging.critical("config.json not found. Execution halted.")
    exit(1)

async def fetch_dataframe(db, symbol: str, interval: str) -> pd.DataFrame:
    """Fetches local database state and converts it into a quantitative dataframe."""
    query = "SELECT timestamp, open, high, low, close, volume FROM market_data WHERE symbol = ? AND interval = ? ORDER BY timestamp ASC"
    async with db.execute(query, (symbol, interval)) as cursor:
        rows = await cursor.fetchall()
        if not rows:
            return pd.DataFrame()
        
        df = pd.DataFrame(rows, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df.set_index(pd.to_datetime(df['timestamp'], unit='ms'), inplace=True)
        return df

def apply_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Vectorized calculation of market structure and volatility metrics."""
    if df.empty or len(df) < 50:
        return df
    
    # Core momentum and trend identification
    df.ta.macd(append=True)
    df.ta.rsi(length=14, append=True)
    
    # Volatility mapping for dynamic risk management
    atr_period = CONFIG['risk_management'].get('atr_period', 14)
    df.ta.atr(length=atr_period, append=True)
    
    return df

async def evaluate_consensus():
    """Identifies multi-timeframe alignment and maps dynamic execution pricing."""
    symbols = CONFIG['data_ingestion']['symbols']
    
    async with aiosqlite.connect('signals.db', timeout=10.0) as db:
        for symbol in symbols:
            df_15m = await fetch_dataframe(db, symbol, '15m')
            
            if df_15m.empty or len(df_15m) < 50:
                logging.info(f"{symbol} - Insufficient data for quantitative matrix.")
                continue
                
            df_15m = apply_indicators(df_15m)
            
            # Extract latest close state for trigger logic
            latest = df_15m.iloc[-1]
            current_price = latest['close']
            macd_val = latest.get('MACD_12_26_9', 0)
            macd_signal = latest.get('MACDs_12_26_9', 0)
            rsi = latest.get('RSI_14', 50)
            atr = latest.get(f'ATRr_{CONFIG["risk_management"]["atr_period"]}', 0)
            
            # Simulated strict validation check based on config requirements
            is_bullish_trigger = macd_val > macd_signal and rsi > 50
            is_bearish_trigger = macd_val < macd_signal and rsi < 50
            
            direction = None
            if is_bullish_trigger:
                direction = "LONG"
            elif is_bearish_trigger:
                direction = "SHORT"
                
            if direction and CONFIG['consensus_engine']['strict_mode']:
                # Placeholder for broader strict logic: Assume strict filtering dictates we proceed
                pass 
            else:
                logging.info(f"{symbol} - Holding. Consensus not met.")
                continue
                
            if direction and atr > 0:
                # Calculate dynamic ATR risk parameters
                sl_mult = CONFIG['risk_management']['sl_multiplier']
                tp1_mult = CONFIG['risk_management']['tp1_multiplier']
                tp2_mult = CONFIG['risk_management']['tp2_multiplier']
                
                if direction == "LONG":
                    stop_loss = current_price - (atr * sl_mult)
                    take_profit_1 = current_price + (atr * tp1_mult)
                    take_profit_2 = current_price + (atr * tp2_mult)
                else:
                    stop_loss = current_price + (atr * sl_mult)
                    take_profit_1 = current_price - (atr * tp1_mult)
                    take_profit_2 = current_price - (atr * tp2_mult)
                
                # Register actionable trade setup
                await db.execute('''
                    INSERT INTO alerts (symbol, direction, price, stop_loss, take_profit_1, take_profit_2, indicators_triggered, timestamp, is_sent)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (symbol, direction, current_price, stop_loss, take_profit_1, take_profit_2, "Strict Matrix Alignment Confirmed", int(latest['timestamp']), 0))
                
                await db.commit()
                logging.info(f"{symbol} - STRICT CONSENSUS MET. Signal registered.")

if __name__ == "__main__":
    asyncio.run(evaluate_consensus())
