import os
import asyncio
import logging
from datetime import datetime, time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Tuple

import pandas as pd
import yfinance as yf
import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Bot
from telegram.error import TelegramError


# =========================
# Configuration
# =========================

BOT_TOKEN = os.getenv("8720565531:AAG5N6qXkFSWlPNixIis5qYYdW_WexQ66D0")
CHAT_ID = os.getenv("5724504948")
IST = pytz.timezone("Asia/Kolkata")

EMA_FAST = 20
EMA_SLOW = 50

MIN_VOLUME_RATIO = 2.5
MIN_BREAKOUT_STRENGTH_PCT = 0.35
MAX_BREAKOUT_STRENGTH_PCT = 3.0

MAX_OPPOSING_WICK_RATIO = 0.90
MIN_BODY_RATIO = 0.05

TOP_STOCKS_LIMIT = 3
MIN_STOCKS_LIMIT = 3
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
        self.debug_stats = {}

    def reset_debug_stats(self):
        self.debug_stats = {
            "total_scanned": 0,
            "data_ok": 0,
            "orb_ok": 0,
            "breakout_ok": 0,
            "ema_ok": 0,
            "vwap_ok": 0,
            "volume_ok": 0,
            "momentum_ok": 0,
            "structure_ok": 0,
            "accepted": 0,
        }

    def increment_stat(self, key: str):
        self.debug_stats[key] = self.debug_stats.get(key, 0) + 1

    def log_rejection(self, symbol: str, reason: str, metrics: Optional[Dict] = None):
        if metrics:
            logger.info(
                "REJECTED %s -> %s | ORB high=%.2f | ORB low=%.2f | confirmation close=%.2f | "
                "breakout strength=%.3f%% | volume ratio=%.2f",
                symbol,
                reason,
                metrics.get("opening_high", 0.0),
                metrics.get("opening_low", 0.0),
                metrics.get("confirmation_close", 0.0),
                metrics.get("active_breakout_strength", 0.0),
                metrics.get("volume_ratio", 0.0),
            )
        else:
            logger.info("REJECTED %s -> %s", symbol, reason)

    def fetch_stock_data(self, symbol: str) -> Optional[pd.DataFrame]:
        """Fetch 5-minute candles with enough history for EMA, VWAP and volume calculations."""
        try:
            data = yf.download(
                symbol + ".NS",
                period="10d",
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
                logger.warning("%s: missing required columns", symbol)
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
            logger.error("Error fetching data for %s: %s", symbol, e)
            return None

    def add_indicators(self, data: pd.DataFrame) -> pd.DataFrame:
        """Add EMA20, EMA50 and session VWAP."""
        data = data.copy()

        data["EMA20"] = data["Close"].ewm(span=EMA_FAST, adjust=False).mean()
        data["EMA50"] = data["Close"].ewm(span=EMA_SLOW, adjust=False).mean()

        typical_price = (data["High"] + data["Low"] + data["Close"]) / 3
        price_volume = typical_price * data["Volume"]

        session = pd.Series(data.index.date, index=data.index)
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

    def get_candle_by_time(
        self,
        today_data: pd.DataFrame,
        start_time: time,
        end_time: time,
    ) -> Optional[pd.Series]:
        """Get a 5-minute candle by its timestamp window."""
        candle_data = today_data[
            (today_data.index.time >= start_time) &
            (today_data.index.time < end_time)
        ]

        if candle_data.empty:
            return None

        return candle_data.iloc[0]

    def get_opening_candle(self, today_data: pd.DataFrame) -> Optional[pd.Series]:
        """Get the completed 9:15-9:20 opening range candle."""
        return self.get_candle_by_time(today_data, time(9, 15), time(9, 20))

    def get_confirmation_candle(self, today_data: pd.DataFrame) -> Optional[pd.Series]:
        """Get the completed 9:20-9:25 confirmation candle."""
        return self.get_candle_by_time(today_data, time(9, 20), time(9, 25))

    def get_previous_first_hour_avg_volume(
        self,
        data: pd.DataFrame,
        today_data: pd.DataFrame,
    ) -> Optional[float]:
        """Average first-hour 5-minute candle volume from previous trading sessions."""
        try:
            today = today_data.index[0].date()
            previous_data = data[data.index.date < today]

            first_hour_data = previous_data[
                (previous_data.index.time >= time(9, 15)) &
                (previous_data.index.time < time(10, 15))
            ]

            if first_hour_data.empty:
                return None

            avg_volume = float(first_hour_data["Volume"].mean())
            return avg_volume if avg_volume > 0 else None

        except Exception:
            return None

    def is_clean_structure(self, candle: pd.Series, direction: str) -> bool:
        """Reject only extremely weak confirmation candles."""
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
            if upper_wick_ratio > MAX_OPPOSING_WICK_RATIO:
                return False

        if direction == "BEARISH":
            if close >= open_price:
                return False
            if lower_wick_ratio > MAX_OPPOSING_WICK_RATIO:
                return False

        return True

    def calculate_score(
        self,
        volume_ratio: float,
        breakout_strength_pct: float,
        ema_aligned: bool,
        vwap_aligned: bool,
        clean_structure: bool,
    ) -> int:
        """Calculate score out of 100."""
        score = 0

        if volume_ratio >= MIN_VOLUME_RATIO:
            score += min(30, int((volume_ratio / 2.5) * 30))

        if breakout_strength_pct > 0:
            score += min(40, int((breakout_strength_pct / 1.0) * 40))
            
        if ema_aligned:
            score += 20

        if vwap_aligned:
            score += 10

        if clean_structure:
            score += 10

        return min(score, 100)

    def calculate_metrics(self, data: pd.DataFrame, today_data: pd.DataFrame) -> Optional[Dict]:
        """Calculate ORB, confirmation candle, trend, volume and breakout metrics."""
        try:
            if data.empty or today_data.empty or len(data) < EMA_SLOW:
                return None

            opening_candle = self.get_opening_candle(today_data)
            confirmation_candle = self.get_confirmation_candle(today_data)

            if opening_candle is None or confirmation_candle is None:
                return None

            previous_close = self.get_previous_close(data, today_data)
            if previous_close is None or previous_close <= 0:
                return None

            today_open = float(today_data["Open"].iloc[0])
            opening_high = float(opening_candle["High"])
            opening_low = float(opening_candle["Low"])

            confirmation_open = float(confirmation_candle["Open"])
            confirmation_high = float(confirmation_candle["High"])
            confirmation_low = float(confirmation_candle["Low"])
            confirmation_close = float(confirmation_candle["Close"])
            confirmation_volume = float(confirmation_candle["Volume"])

            ema20 = float(confirmation_candle["EMA20"])
            ema50 = float(confirmation_candle["EMA50"])
            vwap = float(confirmation_candle["VWAP"])

            if any(pd.isna(x) for x in [today_open, opening_high, opening_low, confirmation_close, ema20, ema50, vwap]):
                return None

            avg_first_hour_volume = self.get_previous_first_hour_avg_volume(data, today_data)
            if avg_first_hour_volume is None:
                return None

            volume_ratio = confirmation_volume / avg_first_hour_volume if avg_first_hour_volume > 0 else 0
            gap_pct = ((today_open - previous_close) / previous_close) * 100

            bullish_breakout_strength = (
                ((confirmation_close - opening_high) / opening_high) * 100
                if opening_high > 0 else 0
            )

            bearish_breakout_strength = (
                ((opening_low - confirmation_close) / opening_low) * 100
                if opening_low > 0 else 0
            )

            active_breakout_strength = max(bullish_breakout_strength, bearish_breakout_strength, 0)

            return {
                "opening_candle": opening_candle,
                "confirmation_candle": confirmation_candle,
                "current_price": confirmation_close,
                "today_open": today_open,
                "previous_close": previous_close,
                "opening_high": opening_high,
                "opening_low": opening_low,
                "confirmation_open": confirmation_open,
                "confirmation_high": confirmation_high,
                "confirmation_low": confirmation_low,
                "confirmation_close": confirmation_close,
                "ema20": ema20,
                "ema50": ema50,
                "vwap": vwap,
                "gap_pct": gap_pct,
                "confirmation_volume": confirmation_volume,
                "avg_first_hour_volume": avg_first_hour_volume,
                "volume_ratio": volume_ratio,
                "bullish_breakout_strength": bullish_breakout_strength,
                "bearish_breakout_strength": bearish_breakout_strength,
                "active_breakout_strength": active_breakout_strength,
            }

        except Exception as e:
            logger.error("Error calculating metrics: %s", e)
            return None

    def check_bullish(self, symbol: str, metrics: Dict) -> Tuple[Optional[Dict], Optional[str]]:
        """Check bullish ORB continuation setup."""
        try:
            confirmation_close = metrics["confirmation_close"]
            breakout_strength = metrics["bullish_breakout_strength"]

            if confirmation_close <= metrics["opening_high"]:
                return None, "failed breakout"

            if not (MIN_BREAKOUT_STRENGTH_PCT <= breakout_strength <= MAX_BREAKOUT_STRENGTH_PCT):
                return None, "failed momentum"

            ema_aligned = metrics["ema20"] > metrics["ema50"]
            if not ema_aligned:
                return None, "failed EMA"

            vwap_aligned = confirmation_close > metrics["vwap"]
            if not vwap_aligned:
                return None, "failed VWAP"

            if metrics["volume_ratio"] < MIN_VOLUME_RATIO:
                return None, "failed volume"

            clean_structure = self.is_clean_structure(metrics["confirmation_candle"], "BULLISH")
            if not clean_structure:
                return None, "failed structure"

            score = self.calculate_score(
                volume_ratio=metrics["volume_ratio"],
                breakout_strength_pct=breakout_strength,
                ema_aligned=ema_aligned,
                vwap_aligned=vwap_aligned,
                clean_structure=clean_structure,
            )

            entry = confirmation_close
            stop_loss = metrics["opening_low"]
            risk = entry - stop_loss

            if risk <= 0:
                return None, "failed risk"

            target = entry + (risk * 2)

            return {
                "symbol": symbol,
                "direction": "BULLISH",
                "entry": round(entry, 2),
                "stop_loss": round(stop_loss, 2),
                "target": round(target, 2),
                "score": score,
                "breakout_strength": round(breakout_strength, 3),
                "volume_ratio": round(metrics["volume_ratio"], 2),
            }, None

        except Exception as e:
            logger.error("Error checking bullish for %s: %s", symbol, e)
            return None, "internal bullish error"

    def check_bearish(self, symbol: str, metrics: Dict) -> Tuple[Optional[Dict], Optional[str]]:
        """Check bearish ORB continuation setup."""
        try:
            confirmation_close = metrics["confirmation_close"]
            breakout_strength = metrics["bearish_breakout_strength"]

            if confirmation_close >= metrics["opening_low"]:
                return None, "failed breakout"

            if not (MIN_BREAKOUT_STRENGTH_PCT <= breakout_strength <= MAX_BREAKOUT_STRENGTH_PCT):
                return None, "failed momentum"

            ema_aligned = metrics["ema20"] < metrics["ema50"]
            if not ema_aligned:
                return None, "failed EMA"

            vwap_aligned = confirmation_close < metrics["vwap"]
            if not vwap_aligned:
                return None, "failed VWAP"

            if metrics["volume_ratio"] < MIN_VOLUME_RATIO:
                return None, "failed volume"

            clean_structure = self.is_clean_structure(metrics["confirmation_candle"], "BEARISH")
            if not clean_structure:
                return None, "failed structure"

            score = self.calculate_score(
                volume_ratio=metrics["volume_ratio"],
                breakout_strength_pct=breakout_strength,
                ema_aligned=ema_aligned,
                vwap_aligned=vwap_aligned,
                clean_structure=clean_structure,
            )

            entry = confirmation_close
            stop_loss = metrics["opening_high"]
            risk = stop_loss - entry

            if risk <= 0:
                return None, "failed risk"

            target = entry - (risk * 2)

            return {
                "symbol": symbol,
                "direction": "BEARISH",
                "entry": round(entry, 2),
                "stop_loss": round(stop_loss, 2),
                "target": round(target, 2),
                "score": score,
                "breakout_strength": round(breakout_strength, 3),
                "volume_ratio": round(metrics["volume_ratio"], 2),
            }, None

        except Exception as e:
            logger.error("Error checking bearish for %s: %s", symbol, e)
            return None, "internal bearish error"

    def get_primary_rejection_reason(self, metrics: Dict) -> str:
        """Return the first strategy filter that failed for debug logging."""
        bullish_breakout = metrics["confirmation_close"] > metrics["opening_high"]
        bearish_breakout = metrics["confirmation_close"] < metrics["opening_low"]

        if not bullish_breakout and not bearish_breakout:
            return "failed breakout"

        if bullish_breakout:
            if metrics["bullish_breakout_strength"] < MIN_BREAKOUT_STRENGTH_PCT:
                return "failed momentum"
            if metrics["bullish_breakout_strength"] > MAX_BREAKOUT_STRENGTH_PCT:
                return "failed momentum"
            if not metrics["ema20"] > metrics["ema50"]:
                return "failed EMA"
            if not metrics["confirmation_close"] > metrics["vwap"]:
                return "failed VWAP"
            if metrics["volume_ratio"] < MIN_VOLUME_RATIO:
                return "failed volume"
            if not self.is_clean_structure(metrics["confirmation_candle"], "BULLISH"):
                return "failed structure"

        if bearish_breakout:
            if metrics["bearish_breakout_strength"] < MIN_BREAKOUT_STRENGTH_PCT:
                return "failed momentum"
            if metrics["bearish_breakout_strength"] > MAX_BREAKOUT_STRENGTH_PCT:
                return "failed momentum"
            if not metrics["ema20"] < metrics["ema50"]:
                return "failed EMA"
            if not metrics["confirmation_close"] < metrics["vwap"]:
                return "failed VWAP"
            if metrics["volume_ratio"] < MIN_VOLUME_RATIO:
                return "failed volume"
            if not self.is_clean_structure(metrics["confirmation_candle"], "BEARISH"):
                return "failed structure"

        return "failed final validation"

    def update_filter_pass_stats(self, metrics: Dict):
        """Track how many stocks passed each broad filter."""
        bullish_breakout = metrics["confirmation_close"] > metrics["opening_high"]
        bearish_breakout = metrics["confirmation_close"] < metrics["opening_low"]

        if bullish_breakout or bearish_breakout:
            self.increment_stat("breakout_ok")

        bullish_ema = bullish_breakout and metrics["ema20"] > metrics["ema50"]
        bearish_ema = bearish_breakout and metrics["ema20"] < metrics["ema50"]

        if bullish_ema or bearish_ema:
            self.increment_stat("ema_ok")

        bullish_vwap = bullish_breakout and metrics["confirmation_close"] > metrics["vwap"]
        bearish_vwap = bearish_breakout and metrics["confirmation_close"] < metrics["vwap"]

        if bullish_vwap or bearish_vwap:
            self.increment_stat("vwap_ok")

        if metrics["volume_ratio"] >= MIN_VOLUME_RATIO:
            self.increment_stat("volume_ok")

        bullish_momentum = MIN_BREAKOUT_STRENGTH_PCT <= metrics["bullish_breakout_strength"] <= MAX_BREAKOUT_STRENGTH_PCT
        bearish_momentum = MIN_BREAKOUT_STRENGTH_PCT <= metrics["bearish_breakout_strength"] <= MAX_BREAKOUT_STRENGTH_PCT

        if (bullish_breakout and bullish_momentum) or (bearish_breakout and bearish_momentum):
            self.increment_stat("momentum_ok")

        bullish_structure = bullish_breakout and self.is_clean_structure(metrics["confirmation_candle"], "BULLISH")
        bearish_structure = bearish_breakout and self.is_clean_structure(metrics["confirmation_candle"], "BEARISH")

        if bullish_structure or bearish_structure:
            self.increment_stat("structure_ok")

    def scan_stock(self, symbol: str) -> Optional[Dict]:
        """Scan one stock for bullish or bearish ORB setup."""
        try:
            self.increment_stat("total_scanned")

            data = self.fetch_stock_data(symbol)
            if data is None or data.empty:
                self.log_rejection(symbol, "no data")
                return None

            data = self.add_indicators(data)
            today_data = self.get_today_data(data)

            if today_data is None or today_data.empty:
                self.log_rejection(symbol, "no today data")
                return None

            self.increment_stat("data_ok")

            metrics = self.calculate_metrics(data, today_data)
            if metrics is None:
                self.log_rejection(symbol, "missing ORB or confirmation candle")
                return None

            self.increment_stat("orb_ok")
            self.update_filter_pass_stats(metrics)

            logger.info(
                "%s DEBUG -> ORB high=%.2f | ORB low=%.2f | confirmation close=%.2f | "
                "bull breakout=%.3f%% | bear breakout=%.3f%% | volume ratio=%.2f | EMA20=%.2f | EMA50=%.2f | VWAP=%.2f",
                symbol,
                metrics["opening_high"],
                metrics["opening_low"],
                metrics["confirmation_close"],
                metrics["bullish_breakout_strength"],
                metrics["bearish_breakout_strength"],
                metrics["volume_ratio"],
                metrics["ema20"],
                metrics["ema50"],
                metrics["vwap"],
            )

            bullish, bullish_rejection = self.check_bullish(symbol, metrics)
            bearish, bearish_rejection = self.check_bearish(symbol, metrics)

            if bullish and bearish:
                self.increment_stat("accepted")
                return bullish if bullish["score"] >= bearish["score"] else bearish

            if bullish:
                self.increment_stat("accepted")
                return bullish

            if bearish:
                self.increment_stat("accepted")
                return bearish

            rejection_reason = self.get_primary_rejection_reason(metrics)
            if rejection_reason == "failed final validation":
                rejection_reason = bullish_rejection or bearish_rejection or rejection_reason

            self.log_rejection(symbol, rejection_reason, metrics)
            return None

        except Exception as e:
            logger.error("Error scanning %s: %s", symbol, e)
            self.log_rejection(symbol, "internal scan error")
            return None

    async def scan_all_stocks(self) -> List[Dict]:
        """Scan all NSE F&O stocks in parallel."""
        self.reset_debug_stats()
        loop = asyncio.get_running_loop()

        tasks = [
            loop.run_in_executor(self.executor, self.scan_stock, symbol)
            for symbol in NSE_FO_STOCKS
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        trades = []

        for result in results:
            if isinstance(result, Exception):
                logger.error("Stock scan task failed: %s", result)
                continue

            if result:
                trades.append(result)

        trades.sort(key=lambda x: x["score"], reverse=True)

        logger.info("SCAN SUMMARY -> total stocks scanned: %s", self.debug_stats.get("total_scanned", 0))
        logger.info("SCAN SUMMARY -> data ok: %s", self.debug_stats.get("data_ok", 0))
        logger.info("SCAN SUMMARY -> ORB/confirmation ok: %s", self.debug_stats.get("orb_ok", 0))
        logger.info("SCAN SUMMARY -> passed breakout: %s", self.debug_stats.get("breakout_ok", 0))
        logger.info("SCAN SUMMARY -> passed EMA: %s", self.debug_stats.get("ema_ok", 0))
        logger.info("SCAN SUMMARY -> passed VWAP: %s", self.debug_stats.get("vwap_ok", 0))
        logger.info("SCAN SUMMARY -> passed volume: %s", self.debug_stats.get("volume_ok", 0))
        logger.info("SCAN SUMMARY -> passed momentum: %s", self.debug_stats.get("momentum_ok", 0))
        logger.info("SCAN SUMMARY -> passed structure: %s", self.debug_stats.get("structure_ok", 0))
        logger.info("SCAN SUMMARY -> accepted setups: %s", self.debug_stats.get("accepted", 0))

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
                for idx, trade in enumerate(trades[:TOP_STOCKS_LIMIT], 1):
                    message += f"{idx}. {trade['symbol']}\n"
                    message += f"   Direction: {trade['direction']}\n"
                    message += f"   Entry: ₹{trade['entry']}\n"
                    message += f"   SL: ₹{trade['stop_loss']}\n"
                    message += f"   Target: ₹{trade['target']}\n"
                    message += f"   Score: {trade['score']}/100\n\n"

                message += "⚠️ Use strict stop loss. Avoid chasing after entry."

            await self.bot.send_message(chat_id=self.chat_id, text=message)
            logger.info("Trade message sent successfully")

        except TelegramError as e:
            logger.error("Telegram error: %s", e)
        except Exception as e:
            logger.error("Error sending message: %s", e)


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
            logger.info("Scan complete: %s high-probability trades found", len(trades))
            await self.notifier.send_trade_message(trades)

        except Exception as e:
            logger.error("Error in scan_and_notify: %s", e)

    def schedule_jobs(self):
        """Schedule the ORB scanning job."""
        self.scheduler.add_job(
            self.scan_and_notify,
            "cron",
            day_of_week="0-4",
            hour=9,
            minute=25,
            timezone=IST,
            id="orb_trade_scan_0925",
            replace_existing=True,
        )

        logger.info("Jobs scheduled successfully")
        logger.info("Bot will scan at 09:25 AM IST, Monday to Friday")

    async def run(self):
        """Start scheduler and keep bot alive."""
        try:
            self.schedule_jobs()
            self.scheduler.start()

            logger.info("🚀 ORB Trade Bot started successfully")

            while True:
                await asyncio.sleep(1)

        except Exception as e:
            logger.error("Error running bot: %s", e)
        finally:
            self.scheduler.shutdown(wait=False)
            self.scanner.executor.shutdown(wait=True)


async def main():
    logger.info("🔥 Starting ORB Trade Bot...")
    bot = TradeBotScheduler()
    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())
