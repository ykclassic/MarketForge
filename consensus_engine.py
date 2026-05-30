import json
import asyncio
import logging
import pandas as pd
import pandas_ta_classic as ta
import aiosqlite

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    with open('config.json', 'r') as f:
        CONFIG = json.load(f)
except FileNotFoundError:
    logging.critical("config.json not found. Execution halted.")
    exit(1)

async def fetch_historical_data(symbol: str, interval: str, limit: int = 100) -> pd.DataFrame:
    async with aiosqlite.connect('signals.db', timeout=10.0) as db:
        query = '''
            SELECT timestamp, open, high, low, close, volume 
            FROM market_data 
            WHERE symbol = ? AND interval = ? 
            ORDER BY timestamp DESC LIMIT ?
        '''
        async with db.execute(query, (symbol, interval, limit)) as cursor:
            rows = await cursor.fetchall()
            
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df = df.sort_values(by='timestamp').reset_index(drop=True)
    return df

def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    bbands = ta.bbands(df['close'], length=20, std=2)
    if bbands is not None:
        df = pd.concat([df, bbands], axis=1)

    macd = ta.macd(df['close'], fast=12, slow=26, signal=9)
    if macd is not None:
        df = pd.concat([df, macd], axis=1)

    df['RSI_14'] = ta.rsi(df['close'], length=14)

    ichimoku, _ = ta.ichimoku(df['high'], df['low'], df['close'], tenkan=9, kijun=26, senkou=52)
    if ichimoku is not None:
        df = pd.concat([df, ichimoku], axis=1)

    return df

def evaluate_consensus(df: pd.DataFrame) -> str:
    if df.empty or len(df) < 52:
        return "NEUTRAL"

    latest = df.iloc[-1]
    
    try:
        close = latest['close']
        rsi = latest['RSI_14']
        macd_line = latest['MACD_12_26_9']
        macd_signal = latest['MACDs_12_26_9']
        senkou_a = latest['ISA_9']
        senkou_b = latest['ISB_26']
        bb_lower = latest['BBL_20_2.0']
        bb_upper = latest['BBU_20_2.0']

        bullish_trend = close > max(senkou_a, senkou_b)
        bullish_mom = macd_line > macd_signal
        bullish_rsi = 40 <= rsi <= 65
        bullish_vol = close < (bb_lower * 1.02) 

        if bullish_trend and bullish_mom and bullish_rsi and bullish_vol:
            return "BULLISH"

        bearish_trend = close < min(senkou_a, senkou_b)
        bearish_mom = macd_line < macd_signal
        bearish_rsi = 35 <= rsi <= 60
        bearish_vol = close > (bb_upper * 0.98) 

        if bearish_trend and bearish_mom and bearish_rsi and bearish_vol:
            return "BEARISH"

    except KeyError as e:
        logging.error(f"Missing indicator data: {e}")
        return "NEUTRAL"

    return "NEUTRAL"

async def process_multi_timeframe(symbol: str):
    intervals = CONFIG['data_ingestion']['intervals']
    required_macros = CONFIG['consensus_engine']['required_macro_alignment']
    results = {}

    for interval in intervals:
        df = await fetch_historical_data(symbol, interval)
        if df.empty:
            return
        df_ta = calculate_indicators(df)
        results[interval] = evaluate_consensus(df_ta)

    # Dynamic Macro Alignment Check
    macro_bullish = all(results.get(tf) == "BULLISH" for tf in required_macros)
    macro_bearish = all(results.get(tf) == "BEARISH" for tf in required_macros)

    # Execution timeframes (assume the lowest timeframe is the entry trigger)
    lowest_tf = intervals[0]

    if macro_bullish and results.get(lowest_tf) != "BEARISH":
        await log_alert(symbol, "BULLISH", "MACD, RSI, Ichimoku, BB (Macro Aligned)")
    elif macro_bearish and results.get(lowest_tf) != "BULLISH":
        await log_alert(symbol, "BEARISH", "MACD, RSI, Ichimoku, BB (Macro Aligned)")
    else:
        logging.info(f"{symbol} - Holding. Consensus not met.")

async def log_alert(symbol: str, direction: str, indicators: str):
    timestamp = int(pd.Timestamp.now(tz='UTC').timestamp() * 1000)
    async with aiosqlite.connect('signals.db', timeout=10.0) as db:
        await db.execute('''
            INSERT INTO alerts (symbol, direction, indicators_triggered, timestamp)
            VALUES (?, ?, ?, ?)
        ''', (symbol, direction, indicators, timestamp))
        await db.commit()
    logging.info(f"*** {direction} SIGNAL LOGGED FOR {symbol} ***")

async def main():
    symbols = CONFIG['data_ingestion']['symbols']
    tasks = [process_multi_timeframe(sym) for sym in symbols]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
