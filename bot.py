import os
import asyncio
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Tuple, Optional

import pandas as pd
import yfinance as yf
import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Bot
from telegram.error import TelegramError

# Configuration
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
IST = pytz.timezone("Asia/Kolkata")

# CHANGED: Centralized strategy thresholds for easier production tuning
EMA_FAST = 20
EMA_SLOW = 50
MIN_VOLUME_RATIO = 1.8
MIN_MOMENTUM_PCT = 0.8
MIN_AVG_RANGE_PCT = 0.04
MIN_EMA_GAP_PCT = 0.01
MAX_EXTENSION_FROM_EMA20_PCT = 1.2

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# NSE F&O stocks
NSE_FO_STOCKS = [
    '360ONE', 'ABB', 'ABCAPITAL', 'ADANIENSOL', 'ADANIENT', 'ADANIGREEN',
    'ADANIPORTS', 'ADANIPOWER', 'ALKEM', 'AMBER', 'AMBUJACEM', 'ANGELONE',
    'APLAPOLLO', 'APOLLOHOSP', 'ASHOKLEY', 'ASIANPAINT', 'ASTRAL', 'AUBANK',
    'AUROPHARMA', 'AXISBANK', 'BAJAJ-AUTO', 'BAJAJFINSV', 'BAJAJHLDNG',
    'BAJFINANCE', 'BANDHANBNK', 'BANKBARODA', 'BDL', 'BEL', 'BHARATFORG',
    'BHARTIARTL', 'BHEL', 'BIOCON', 'BLUESTARCO', 'BOSCHLTD', 'BPCL',
    'BRITANNIA', 'BSE', 'CAMS', 'CANBK', 'CDSL', 'CHOLAFIN', 'CIPLA',
    'COALINDIA', 'COCHINSHIP', 'COFORGE', 'COLPAL', 'CONCOR', 'CROMPTON',
    'CUMMINSIND', 'DABUR', 'DALBHARAT', 'DELHIVERY', 'DIVISLAB', 'DIXON',
    'DLF', 'DMART', 'DRREDDY', 'EICHERMOT', 'ETERNAL', 'EXIDEIND',
    'FEDERALBNK', 'FORCEMOT', 'FORTIS', 'GAIL', 'GLENMARK', 'GMRAIRPORT',
    'GODFRYPHLP', 'GODREJCP', 'GODREJPROP', 'GRASIM', 'HAL', 'HAVELLS',
    'HCLTECH', 'HDFCAMC', 'HDFCBANK', 'HDFCLIFE', 'HEROMOTOCO', 'HINDALCO',
    'HINDPETRO', 'HINDUNILVR', 'HINDZINC', 'HUDCO', 'HYUNDAI', 'ICICIBANK',
    'ICICIGI', 'ICICIPRULI', 'IDEA', 'IDFCFIRSTB', 'IEX', 'INDHOTEL',
    'INDIANB', 'INDIGO', 'INDUSINDBK', 'INDUSTOWER', 'INFY', 'INOXWIND',
    'IOC', 'IREDA', 'IRFC', 'ITC', 'JINDALSTEL', 'JIOFIN', 'JSWENERGY',
    'JSWSTEEL', 'JUBLFOOD', 'KALYANKJIL', 'KAYNES', 'KEI', 'KFINTECH',
    'KOTAKBANK', 'KPITTECH', 'LAURUSLABS', 'LICHSGFIN', 'LICI', 'LODHA',
    'LT', 'LTF', 'LTM', 'LUPIN', 'M&M', 'MANAPPURAM', 'MANKIND', 'MARICO',
    'MARUTI', 'MAXHEALTH', 'MAZDOCK', 'MCX', 'MFSL', 'MOTHERSON',
    'MOTILALOFS', 'MPHASIS', 'MUTHOOTFIN', 'NAM-INDIA', 'NATIONALUM',
    'NAUKRI', 'NBCC', 'NESTLEIND', 'NHPC', 'NMDC', 'NTPC', 'NUVAMA',
    'NYKAA', 'OBEROIRLTY', 'OFSS', 'OIL', 'ONGC', 'PAGEIND', 'PATANJALI',
    'PAYTM', 'PERSISTENT', 'PETRONET', 'PFC', 'PGEL', 'PHOENIXLTD',
    'PIDILITIND', 'PIIND', 'PNB', 'PNBHOUSING', 'POLICYBZR', 'POLYCAB',
    'POWERGRID', 'POWERINDIA', 'PPLPHARMA', 'PREMIERENE', 'PRESTIGE',
    'RBLBANK', 'RECLTD', 'RVNL', 'SAIL', 'SAMMAANCAP', 'SBICARD',
    'SBILIFE', 'SBIN', 'SHREECEM', 'SHRIRAMFIN', 'SIEMENS', 'SOLARINDS',
    'SONACOMS', 'SRF', 'SUNPHARMA', 'SUPREMEIND', 'SUZLON', 'SWIGGY',
    'TATACONSUM', 'TATAELXSI', 'TATAPOWER', 'TATASTEEL', 'TATATECH',
    'TCS', 'TECHM', 'TIINDIA', 'TMPV', 'TITAN', 'TORNTPHARM',
    'TORNTPOWER', 'TRENT', 'TVSMOTOR', 'ULTRACEMCO', 'UNIONBANK',
    'UNITDSPR', 'UNOMINDA', 'UPL', 'VBL', 'VEDL', 'VMM', 'VOLTAS',
    'WAAREEENER', 'WIPRO', 'YESBANK', 'ZYDUSLIFE'
]


class TradeScanner:
    """Scanner for NSE F&O intraday trend-continuation signals"""

    def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=10)

    def fetch_stock_data(self, symbol: str) -> Optional[pd.DataFrame]:
        """Fetch 5-minute data with enough history for EMA20/EMA50."""
        try:
            # CHANGED: period increased from 1d to 5d so EMA50 has enough candles.
            data = yf.download(
                symbol + ".NS",
                period="5d",
                interval="5m",
                progress=False,
                prepost=False,
                auto_adjust=False,
                threads=False,
            )

            if data.empty:
                return None

            if isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)

            data = data[~data.index.duplicated(keep="first")]
            data = data.sort_index()

            # CHANGED: Normalize timestamps to IST for accurate 9:25 AM logic.
            if data.index.tz is None:
                data.index = data.index.tz_localize(IST)
            else:
                data.index = data.index.tz_convert(IST)

            return data

        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {e}")
            return None

    def add_indicators(self, data: pd.DataFrame) -> pd.DataFrame:
        """Add EMA20 and EMA50 indicators."""
        # CHANGED: Added EMA20/EMA50 trend confirmation.
        data = data.copy()
        data["EMA20"] = data["Close"].ewm(span=EMA_FAST, adjust=False).mean()
        data["EMA50"] = data["Close"].ewm(span=EMA_SLOW, adjust=False).mean()
        data["VWAP"] = (data["Volume"] * (data["High"] + data["Low"] + data["Close"]) / 3).cumsum() / data["Volume"].cumsum()
        return data

    def get_today_data(self, data: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Return only today's market data."""
        today = datetime.now(IST).date()
        today_data = data[data.index.date == today]

        if today_data.empty:
            logger.info("No current-day data available yet.")
            return None

        return today_data

    def calculate_metrics(self, data: pd.DataFrame, today_data: pd.DataFrame) -> Optional[Dict]:
        """Calculate trend-continuation metrics."""
        try:
            # CHANGED: Need enough total candles for reliable EMA50 and 3 current candles.
            if data.empty or today_data.empty or len(data) < EMA_SLOW or len(today_data) < 3:
                return None

            last_3 = today_data.tail(2)
            latest = last_3.iloc[-1]
            previous_data = data.loc[data.index < latest.name].tail(20)
            vwap = float(latest["VWAP"])

            if previous_data.empty:
                return None

            current_price = float(latest["Close"])
            open_price = float(today_data["Open"].iloc[0])
            high = float(latest["High"])
            low = float(latest["Low"])
            close = float(latest["Close"])
            ema20 = float(latest["EMA20"])
            ema50 = float(latest["EMA50"])

            if any(pd.isna(x) for x in [current_price, open_price, high, low, close, ema20, ema50]):
                return None

            momentum_pct = ((current_price - open_price) / open_price) * 100

            current_volume = float(latest["Volume"])
            avg_volume = float(previous_data["Volume"].mean())
            volume_ratio = current_volume / avg_volume if avg_volume > 0 else 0

            # CHANGED: Last 3 candles must show clean trend structure.
            highs = last_3["High"].astype(float).tolist()
            lows = last_3["Low"].astype(float).tolist()
            closes = last_3["Close"].astype(float).tolist()
            ema20_values = last_3["EMA20"].astype(float).tolist()

            higher_highs = highs[0] < highs[1] < highs[2]
            higher_lows = lows[0] < lows[1] < lows[2]
            lower_highs = highs[0] > highs[1] > highs[2]
            lower_lows = lows[0] > lows[1] > lows[2]

            price_above_ema20 = all(c > e for c, e in zip(closes, ema20_values))
            price_below_ema20 = all(c < e for c, e in zip(closes, ema20_values))

            ema_gap_pct = abs((ema20 - ema50) / ema50) * 100 if ema50 > 0 else 0
            extension_from_ema20_pct = abs((current_price - ema20) / ema20) * 100 if ema20 > 0 else 0

            ranges = ((last_3["High"] - last_3["Low"]) / last_3["Close"]) * 100
            avg_range_pct = float(ranges.mean())

            candle_range = high - low
            close_position = ((close - low) / candle_range) if candle_range > 0 else 0.5

            return {
                "current_price": current_price,
                "open_price": open_price,
                "high": high,
                "low": low,
                "close": close,
                "ema20": ema20,
                "ema50": ema50,
                "momentum_pct": momentum_pct,
                "current_volume": current_volume,
                "avg_volume": avg_volume,
                "volume_ratio": volume_ratio,
                "higher_highs": higher_highs,
                "higher_lows": higher_lows,
                "lower_highs": lower_highs,
                "lower_lows": lower_lows,
                "price_above_ema20": price_above_ema20,
                "price_below_ema20": price_below_ema20,
                "ema_gap_pct": ema_gap_pct,
                "extension_from_ema20_pct": extension_from_ema20_pct,
                "avg_range_pct": avg_range_pct,
                "close_position": close_position,
                "vwap": vwap,
            }

        except Exception as e:
            logger.error(f"Error calculating metrics: {e}")
            return None

    def check_bullish(self, symbol: str, metrics: Dict) -> Optional[Dict]:
        """Check if stock meets high-probability bullish continuation conditions."""
        try:
            if not metrics:
                return None

            # CHANGED: Early trend-continuation logic replaces late ORB breakout logic.
            if (
                metrics["ema20"] > metrics["ema50"]
                and metrics["price_above_ema20"]
                and (
                    metrics["higher_highs"]
                    or metrics["higher_lows"]
                    or metrics["momentum_pct"] > 0.8
                )
                and metrics["volume_ratio"] >= MIN_VOLUME_RATIO
                and metrics["momentum_pct"] >= MIN_MOMENTUM_PCT
                and metrics["avg_range_pct"] >= MIN_AVG_RANGE_PCT
                and metrics["ema_gap_pct"] >= MIN_EMA_GAP_PCT
                and metrics["extension_from_ema20_pct"] <= MAX_EXTENSION_FROM_EMA20_PCT
                and metrics["close_position"] >= 0.60
                and metrics["current_price"] > metrics["vwap"]
            ):
                entry = metrics["current_price"]
                stop_loss = entry * 0.995
                target = entry * 1.015

                # Skip already pumped stocks
                if metrics["momentum_pct"] > 2.2:
                    return None

                return {
                    "symbol": symbol,
                    "entry": round(entry, 2),
                    "stop_loss": round(stop_loss, 2),
                    "target": round(target, 2),
                    "momentum": round(metrics["momentum_pct"], 2),
                }

            return None

        except Exception as e:
            logger.error(f"Error checking bullish for {symbol}: {e}")
            return None

    def check_bearish(self, symbol: str, metrics: Dict) -> Optional[Dict]:
        """Check if stock meets high-probability bearish continuation conditions."""
        try:
            if not metrics:
                return None

            # CHANGED: Symmetric bearish continuation logic.
            if (
                metrics["ema20"] < metrics["ema50"]
                and metrics["price_below_ema20"]
                and (
                    metrics["lower_highs"]
                    or metrics["lower_lows"]
                    or metrics["momentum_pct"] < -0.6
                )
                and metrics["volume_ratio"] >= MIN_VOLUME_RATIO
                and metrics["momentum_pct"] <= -MIN_MOMENTUM_PCT
                and metrics["avg_range_pct"] >= MIN_AVG_RANGE_PCT
                and metrics["ema_gap_pct"] >= MIN_EMA_GAP_PCT
                and metrics["extension_from_ema20_pct"] <= MAX_EXTENSION_FROM_EMA20_PCT
                and metrics["close_position"] <= 0.40
                and metrics["current_price"] < metrics["vwap"]
            ):
                entry = metrics["current_price"]
                stop_loss = entry * 1.005
                target = entry * 0.99

                return {
                    "symbol": symbol,
                    "entry": round(entry, 2),
                    "stop_loss": round(stop_loss, 2),
                    "target": round(target, 2),
                    "momentum": round(metrics["momentum_pct"], 2),
                }

            return None

        except Exception as e:
            logger.error(f"Error checking bearish for {symbol}: {e}")
            return None

    def scan_stock(self, symbol: str) -> Tuple[Optional[Dict], Optional[Dict]]:
        """Scan a single stock for bullish and bearish setups."""
        try:
            data = self.fetch_stock_data(symbol)
            if data is None or data.empty:
                return None, None

            data = self.add_indicators(data)
            today_data = self.get_today_data(data)

            if today_data is None or today_data.empty:
                return None, None

            metrics = self.calculate_metrics(data, today_data)
            if metrics is None:
                return None, None

            bullish = self.check_bullish(symbol, metrics)
            bearish = self.check_bearish(symbol, metrics)

            return bullish, bearish

        except Exception as e:
            logger.error(f"Error scanning {symbol}: {e}")
            return None, None

    async def scan_all_stocks(self) -> Tuple[List[Dict], List[Dict]]:
        """Scan all stocks in parallel."""
        loop = asyncio.get_running_loop()
        bullish_trades = []
        bearish_trades = []

        tasks = [
            loop.run_in_executor(self.executor, self.scan_stock, symbol)
            for symbol in NSE_FO_STOCKS
        ]

        # CHANGED: return_exceptions=True prevents one symbol failure from killing the scan.
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Stock scan task failed: {result}")
                continue

            bullish, bearish = result

            if bullish:
                bullish_trades.append(bullish)
            if bearish:
                bearish_trades.append(bearish)

        bullish_trades.sort(key=lambda x: x["momentum"], reverse=True)
        bearish_trades.sort(key=lambda x: x["momentum"])

        return bullish_trades[:5], bearish_trades[:5]


class TelegramNotifier:
    """Handle Telegram notifications."""

    def __init__(self, bot_token: str, chat_id: str):
        # CHANGED: Validate both Railway environment variables.
        if not bot_token:
            raise ValueError("BOT_TOKEN is missing")
        if not chat_id:
            raise ValueError("CHAT_ID is missing")

        self.bot = Bot(token=bot_token)
        self.chat_id = chat_id

    async def send_trade_message(
        self, bullish_trades: List[Dict], bearish_trades: List[Dict]
    ):
        """Send formatted trade setup message."""
        try:
            current_time = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")

            message = "📊 NSE F&O TRADE SETUPS\n"
            message += f"⏰ Time: {current_time}\n\n"

            if bullish_trades:
                message += "🔥 TOP 5 BULLISH TRADES:\n\n"
                for idx, trade in enumerate(bullish_trades, 1):
                    message += f"{idx}. {trade['symbol'].replace('.NS', '')}\n"
                    message += f"   Entry: ₹{trade['entry']}\n"
                    message += f"   SL: ₹{trade['stop_loss']}\n"
                    message += f"   Target: ₹{trade['target']}\n"
                    message += f"   Momentum: {trade['momentum']}%\n\n"
            else:
                message += "🔥 TOP 5 BULLISH TRADES:\n   No signals\n\n"

            if bearish_trades:
                message += "🔻 TOP 5 BEARISH TRADES:\n\n"
                for idx, trade in enumerate(bearish_trades, 1):
                    message += f"{idx}. {trade['symbol'].replace('.NS', '')}\n"
                    message += f"   Entry: ₹{trade['entry']}\n"
                    message += f"   SL: ₹{trade['stop_loss']}\n"
                    message += f"   Target: ₹{trade['target']}\n"
                    message += f"   Momentum: {trade['momentum']}%\n\n"
            else:
                message += "🔻 TOP 5 BEARISH TRADES:\n   No signals\n\n"

            message += "⚠️ Risk Management Rules: Always use stop loss\n"
            message += "Always take profit at target levels\n"

            await self.bot.send_message(chat_id=self.chat_id, text=message)
            logger.info("Trade message sent successfully")

        except TelegramError as e:
            logger.error(f"Telegram error: {e}")
        except Exception as e:
            logger.error(f"Error sending message: {e}")


class TradeBotScheduler:
    """Main scheduler and bot manager."""

    def __init__(self):
        self.scanner = TradeScanner()
        self.notifier = TelegramNotifier(BOT_TOKEN, CHAT_ID)
        self.scheduler = AsyncIOScheduler(timezone=IST)

    async def scan_and_notify(self):
        """Scan stocks and send notification."""
        try:
            logger.info("Starting market scan...")
            bullish_trades, bearish_trades = await self.scanner.scan_all_stocks()
            logger.info(
                f"Scan complete: {len(bullish_trades)} bullish, {len(bearish_trades)} bearish"
            )
            await self.notifier.send_trade_message(bullish_trades, bearish_trades)

        except Exception as e:
            logger.error(f"Error in scan_and_notify: {e}")

    def schedule_jobs(self):
        """Schedule the scanning job."""
        # Scheduler kept exactly at 09:25 AM IST, Monday to Friday.
        self.scheduler.add_job(
            self.scan_and_notify,
            "cron",
            day_of_week="0-4",
            hour=9,
            minute=18,
            timezone=IST,
            id="trade_scan_0925",
            replace_existing=True,
        )

        logger.info("Jobs scheduled successfully")
        logger.info("Bot will scan at 09:25 AM IST (Mon-Fri)")

    async def run(self):
        try:
            self.schedule_jobs()
            self.scheduler.start()

            logger.info("🚀 Trade bot started successfully")

            while True:
                await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Error running bot: {e}")
        finally:
            self.scheduler.shutdown()
            self.scanner.executor.shutdown(wait=True)


async def main():
    logger.info("🔥 Starting Trade Bot...")
    bot = TradeBotScheduler()
    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())
