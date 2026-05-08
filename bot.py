import os
import asyncio
import logging
from datetime import datetime, time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Tuple, Optional

import pandas as pd
import yfinance as yf
import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Bot
from telegram.error import TelegramError


# =========================
# Configuration
# =========================

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
IST = pytz.timezone("Asia/Kolkata")

EMA_FAST = 20
EMA_SLOW = 50

MIN_GAP_PCT = 0.8
MIN_VOLUME_RATIO = 1.5
MIN_MOMENTUM_PCT = 0.8
MAX_MOMENTUM_PCT = 2.5

MAX_WICK_RATIO = 0.45
MIN_BODY_RATIO = 0.35

TOP_STOCKS_LIMIT = 5
MAX_WORKERS = 10


# =========================
# Logging
# =========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# =========================
# NSE F&O stocks
# =========================

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
    """Scanner for NSE F&O ORB and early momentum signals."""

    def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

    def fetch_stock_data(self, symbol: str) -> Optional[pd.DataFrame]:
        """Fetch 5-minute candles with enough history for EMA calculations."""
        try:
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

            required_columns = {"Open", "High", "Low", "Close", "Volume"}
            if not required_columns.issubset(data.columns):
                logger.warning(f"{symbol}: missing required columns")
                return None

            data = data.dropna(subset=["Open", "High", "Low", "Close"])
            data = data[~data.index.duplicated(keep="first")]
            data = data.sort_index()

            if data.index.tz is None:
                data.index = data.index.tz_localize("UTC").tz_convert(IST)
            else:
                data.index = data.index.tz_convert(IST)

            return data

        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {e}")
            return None

    def add_indicators(self, data: pd.DataFrame) -> pd.DataFrame:
        """Add EMA20, EMA50 and session VWAP."""
        data = data.copy()

        data["EMA20"] = data["Close"].ewm(span=EMA_FAST, adjust=False).mean()
        data["EMA50"] = data["Close"].ewm(span=EMA_SLOW, adjust=False).mean()

        typical_price = (data["High"] + data["Low"] + data["Close"]) / 3
        price_volume = typical_price * data["Volume"]

        session = data.index.date
        data["PV_CUM"] = price_volume.groupby(session).cumsum()
        data["VOL_CUM"] = data["Volume"].groupby(session).cumsum()
        data["VWAP"] = data["PV_CUM"] / data["VOL_CUM"].replace(0, pd.NA)

        data = data.drop(columns=["PV_CUM", "VOL_CUM"])

        return data

    def get_today_data(self, data: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Return today's market data."""
        today = datetime.now(IST).date()
        today_data = data[data.index.date == today]

        if today_data.empty:
            return None

        return today_data

    def get_previous_close(self, data: pd.DataFrame, today_data: pd.DataFrame) -> Optional[float]:
        """Get previous trading session close."""
        try:
            first_today_time = today_data.index.min()
            previous_data = data[data.index < first_today_time]

            if previous_data.empty:
                return None

            return float(previous_data["Close"].iloc[-1])

        except Exception:
            return None

    def get_opening_candle(self, today_data: pd.DataFrame) -> Optional[pd.Series]:
        """Get the 9:15-9:20 opening range candle."""
        opening_data = today_data[
            (today_data.index.time >= time(9, 15)) &
            (today_data.index.time < time(9, 20))
        ]

        if opening_data.empty:
            return None

        return opening_data.iloc[0]

    def is_clean_structure(self, candle: pd.Series, direction: str) -> bool:
        """Reject weak candles, long wicks and sideways structures."""
        high = float(candle["High"])
        low = float(candle["Low"])
        open_price = float(candle["Open"])
        close = float(candle["Close"])

        candle_range = high - low
        body = abs(close - open_price)

        if candle_range <= 0:
            return False

        body_ratio = body / candle_range
        upper_wick_ratio = (high - max(open_price, close)) / candle_range
        lower_wick_ratio = (min(open_price, close) - low) / candle_range

        if body_ratio < MIN_BODY_RATIO:
            return False

        if direction == "BULLISH":
            if close <= open_price:
                return False
            if upper_wick_ratio > MAX_WICK_RATIO:
                return False

        if direction == "BEARISH":
            if close >= open_price:
                return False
            if lower_wick_ratio > MAX_WICK_RATIO:
                return False

        return True

    def calculate_score(
        self,
        gap_pct: float,
        volume_ratio: float,
        breakout_strength_pct: float,
        ema_aligned: bool,
        clean_structure: bool,
    ) -> int:
        """Calculate score out of 100."""
        score = 0

        if volume_ratio >= MIN_VOLUME_RATIO:
            score += min(30, int((volume_ratio / 3.0) * 30))

        if abs(gap_pct) >= MIN_GAP_PCT:
            score += min(20, int((abs(gap_pct) / 2.0) * 20))

        if breakout_strength_pct > 0:
            score += min(25, int((breakout_strength_pct / 1.0) * 25))

        if ema_aligned:
            score += 15

        if clean_structure:
            score += 10

        return min(score, 100)

    def calculate_metrics(self, data: pd.DataFrame, today_data: pd.DataFrame) -> Optional[Dict]:
        """Calculate ORB, gap, trend, volume and momentum metrics."""
        try:
            if data.empty or today_data.empty or len(data) < EMA_SLOW:
                return None

            opening_candle = self.get_opening_candle(today_data)
            if opening_candle is None:
                return None

            candles_after_opening = today_data[today_data.index > opening_candle.name]
            if candles_after_opening.empty:
                return None

            latest = candles_after_opening.iloc[-1]

            previous_close = self.get_previous_close(data, today_data)
            if previous_close is None or previous_close <= 0:
                return None

            today_open = float(today_data["Open"].iloc[0])
            current_price = float(latest["Close"])
            opening_high = float(opening_candle["High"])
            opening_low = float(opening_candle["Low"])

            ema20 = float(latest["EMA20"])
            ema50 = float(latest["EMA50"])
            vwap = float(latest["VWAP"])

            if any(pd.isna(x) for x in [today_open, current_price, ema20, ema50, vwap]):
                return None

            gap_pct = ((today_open - previous_close) / previous_close) * 100
            momentum_pct = ((current_price - today_open) / today_open) * 100

            previous_volume_data = data[data.index < latest.name].tail(20)
            if previous_volume_data.empty:
                return None

            current_volume = float(latest["Volume"])
            avg_volume = float(previous_volume_data["Volume"].mean())
            volume_ratio = current_volume / avg_volume if avg_volume > 0 else 0

            bullish_breakout_strength = (
                ((current_price - opening_high) / opening_high) * 100
                if opening_high > 0 else 0
            )

            bearish_breakout_strength = (
                ((opening_low - current_price) / opening_low) * 100
                if opening_low > 0 else 0
            )

            recent_candles = today_data.tail(3)
            higher_closes = recent_candles["Close"].is_monotonic_increasing
            lower_closes = recent_candles["Close"].is_monotonic_decreasing

            return {
                "latest": latest,
                "current_price": current_price,
                "today_open": today_open,
                "previous_close": previous_close,
                "opening_high": opening_high,
                "opening_low": opening_low,
                "ema20": ema20,
                "ema50": ema50,
                "vwap": vwap,
                "gap_pct": gap_pct,
                "momentum_pct": momentum_pct,
                "current_volume": current_volume,
                "avg_volume": avg_volume,
                "volume_ratio": volume_ratio,
                "bullish_breakout_strength": bullish_breakout_strength,
                "bearish_breakout_strength": bearish_breakout_strength,
                "higher_closes": higher_closes,
                "lower_closes": lower_closes,
            }

        except Exception as e:
            logger.error(f"Error calculating metrics: {e}")
            return None

    def check_bullish(self, symbol: str, metrics: Dict) -> Optional[Dict]:
        """Check bullish ORB continuation setup."""
        try:
            if not metrics:
                return None

            gap_pct = metrics["gap_pct"]
            momentum_pct = metrics["momentum_pct"]

            if gap_pct <= MIN_GAP_PCT:
                return None

            if not (MIN_MOMENTUM_PCT <= momentum_pct <= MAX_MOMENTUM_PCT):
                return None

            ema_aligned = metrics["ema20"] > metrics["ema50"]
            clean_structure = self.is_clean_structure(metrics["latest"], "BULLISH")

            if (
                metrics["current_price"] > metrics["opening_high"]
                and ema_aligned
                and metrics["current_price"] > metrics["vwap"]
                and metrics["volume_ratio"] >= MIN_VOLUME_RATIO
                and metrics["higher_closes"]
                and clean_structure
            ):
                score = self.calculate_score(
                    gap_pct=gap_pct,
                    volume_ratio=metrics["volume_ratio"],
                    breakout_strength_pct=metrics["bullish_breakout_strength"],
                    ema_aligned=ema_aligned,
                    clean_structure=clean_structure,
                )

                entry = metrics["current_price"]
                stop_loss = metrics["opening_low"]
                risk = entry - stop_loss

                if risk <= 0:
                    return None

                target = entry + (risk * 2)

                return {
                    "symbol": symbol,
                    "direction": "BULLISH",
                    "entry": round(entry, 2),
                    "stop_loss": round(stop_loss, 2),
                    "target": round(target, 2),
                    "score": score,
                }

            return None

        except Exception as e:
            logger.error(f"Error checking bullish for {symbol}: {e}")
            return None

    def check_bearish(self, symbol: str, metrics: Dict) -> Optional[Dict]:
        """Check bearish ORB continuation setup."""
        try:
            if not metrics:
                return None

            gap_pct = metrics["gap_pct"]
            momentum_pct = metrics["momentum_pct"]

            if gap_pct >= -MIN_GAP_PCT:
                return None

            if not (-MAX_MOMENTUM_PCT <= momentum_pct <= -MIN_MOMENTUM_PCT):
                return None

            ema_aligned = metrics["ema20"] < metrics["ema50"]
            clean_structure = self.is_clean_structure(metrics["latest"], "BEARISH")

            if (
                metrics["current_price"] < metrics["opening_low"]
                and ema_aligned
                and metrics["current_price"] < metrics["vwap"]
                and metrics["volume_ratio"] >= MIN_VOLUME_RATIO
                and metrics["lower_closes"]
                and clean_structure
            ):
                score = self.calculate_score(
                    gap_pct=gap_pct,
                    volume_ratio=metrics["volume_ratio"],
                    breakout_strength_pct=metrics["bearish_breakout_strength"],
                    ema_aligned=ema_aligned,
                    clean_structure=clean_structure,
                )

                entry = metrics["current_price"]
                stop_loss = metrics["opening_high"]
                risk = stop_loss - entry

                if risk <= 0:
                    return None

                target = entry - (risk * 2)

                return {
                    "symbol": symbol,
                    "direction": "BEARISH",
                    "entry": round(entry, 2),
                    "stop_loss": round(stop_loss, 2),
                    "target": round(target, 2),
                    "score": score,
                }

            return None

        except Exception as e:
            logger.error(f"Error checking bearish for {symbol}: {e}")
            return None

    def scan_stock(self, symbol: str) -> Optional[Dict]:
        """Scan one stock for bullish or bearish ORB setup."""
        try:
            data = self.fetch_stock_data(symbol)
            if data is None or data.empty:
                return None

            data = self.add_indicators(data)
            today_data = self.get_today_data(data)

            if today_data is None or today_data.empty:
                return None

            metrics = self.calculate_metrics(data, today_data)
            if metrics is None:
                return None

            bullish = self.check_bullish(symbol, metrics)
            bearish = self.check_bearish(symbol, metrics)

            if bullish and bearish:
                return bullish if bullish["score"] >= bearish["score"] else bearish

            return bullish or bearish

        except Exception as e:
            logger.error(f"Error scanning {symbol}: {e}")
            return None

    async def scan_all_stocks(self) -> List[Dict]:
        """Scan all NSE F&O stocks in parallel."""
        loop = asyncio.get_running_loop()

        tasks = [
            loop.run_in_executor(self.executor, self.scan_stock, symbol)
            for symbol in NSE_FO_STOCKS
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        trades = []

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Stock scan task failed: {result}")
                continue

            if result:
                trades.append(result)

        trades.sort(key=lambda x: x["score"], reverse=True)

        return trades[:TOP_STOCKS_LIMIT]


class TelegramNotifier:
    """Handle Telegram notifications."""

    def __init__(self, bot_token: str, chat_id: str):
        if not bot_token:
            raise ValueError("BOT_TOKEN is missing")
        if not chat_id:
            raise ValueError("CHAT_ID is missing")

        self.bot = Bot(token=bot_token)
        self.chat_id = chat_id

    async def send_trade_message(self, trades: List[Dict]):
        """Send formatted ORB trade setup message."""
        try:
            message = "📊 HIGH PROBABILITY STOCKS (9:25 AM)\n\n"

            if not trades:
                message += "No high-probability ORB setups found today.\n"
                message += "\n⚠️ No trade is better than a weak trade."
            else:
                for idx, trade in enumerate(trades, 1):
                    direction_icon = "🟢" if trade["direction"] == "BULLISH" else "🔴"

                    message += f"{idx}. {direction_icon} {trade['symbol']}\n"
                    message += f"   Entry: ₹{trade['entry']}\n"
                    message += f"   SL: ₹{trade['stop_loss']}\n"
                    message += f"   Target: ₹{trade['target']}\n"
                    message += f"   Score: {trade['score']}/100\n\n"

                message += "⚠️ Use strict stop loss. Avoid chasing after entry."

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
            logger.info("Starting ORB market scan...")
            trades = await self.scanner.scan_all_stocks()
            logger.info(f"Scan complete: {len(trades)} high-probability trades found")
            await self.notifier.send_trade_message(trades)

        except Exception as e:
            logger.error(f"Error in scan_and_notify: {e}")

    def schedule_jobs(self):
        """Schedule the ORB scanning job."""
        self.scheduler.add_job(
            self.scan_and_notify,
            "cron",
            day_of_week="0-4",
            hour=9,
            minute=20,
            timezone=IST,
            id="orb_trade_scan_0920",
            replace_existing=True,
        )

        logger.info("Jobs scheduled successfully")
        logger.info("Bot will scan at 09:20 AM IST, Monday to Friday")

    async def run(self):
        """Start scheduler and keep bot alive."""
        try:
            self.schedule_jobs()
            self.scheduler.start()

            logger.info("🚀 ORB Trade Bot started successfully")

            while True:
                await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Error running bot: {e}")
        finally:
            self.scheduler.shutdown(wait=False)
            self.scanner.executor.shutdown(wait=True)


async def main():
    logger.info("🔥 Starting ORB Trade Bot...")
    bot = TradeBotScheduler()
    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())
