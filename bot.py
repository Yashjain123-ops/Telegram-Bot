import os
import asyncio
import logging
from datetime import datetime, time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Tuple

import pandas as pd
import numpy as np
import yfinance as yf
import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Bot
from telegram.error import TelegramError

# Configuration
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
IST = pytz.timezone("Asia/Kolkata")

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# NSE F&O stocks (common intraday trading stocks)
NSE_FO_STOCKS = [
    '360ONE',
            'ABB',
            'ABCAPITAL',
            'ADANIENSOL',
            'ADANIENT',
            'ADANIGREEN',
            'ADANIPORTS',
            'ADANIPOWER',
            'ALKEM',
            'AMBER',
            'AMBUJACEM',
            'ANGELONE',
            'APLAPOLLO',
            'APOLLOHOSP',
            'ASHOKLEY',
            'ASIANPAINT',
            'ASTRAL',
            'AUBANK',
            'AUROPHARMA',
            'AXISBANK',

            'BAJAJ-AUTO',
            'BAJAJFINSV',
            'BAJAJHLDNG',
            'BAJFINANCE',
            'BANDHANBNK',
            'BANKBARODA',
            'BDL',
            'BEL',
            'BHARATFORG',
            'BHARTIARTL',
            'BHEL',
            'BIOCON',
            'BLUESTARCO',
            'BOSCHLTD',
            'BPCL',
            'BRITANNIA',
            'BSE',

            'CAMS',
            'CANBK',
            'CDSL',
            'CHOLAFIN',
            'CIPLA',
            'COALINDIA',
            'COCHINSHIP',
            'COFORGE',
            'COLPAL',
            'CONCOR',
            'CROMPTON',
            'CUMMINSIND',

            'DABUR',
            'DALBHARAT',
            'DELHIVERY',
            'DIVISLAB',
            'DIXON',
            'DLF',
            'DMART',
            'DRREDDY',

            'EICHERMOT',
            'ETERNAL',
            'EXIDEIND',

            'FEDERALBNK',
            'FORCEMOT',
            'FORTIS',

            'GAIL',
            'GLENMARK',
            'GMRAIRPORT',
            'GODFRYPHLP',
            'GODREJCP',
            'GODREJPROP',
            'GRASIM',

            'HAL',
            'HAVELLS',
            'HCLTECH',
            'HDFCAMC',
            'HDFCBANK',
            'HDFCLIFE',
            'HEROMOTOCO',
            'HINDALCO',
            'HINDPETRO',
            'HINDUNILVR',
            'HINDZINC',
            'HUDCO',
            'HYUNDAI',
            
            'ICICIBANK',
            'ICICIGI',
            'ICICIPRULI',
            'IDEA',
            'IDFCFIRSTB',
            'IEX',
            'INDHOTEL',
            'INDIANB',
            'INDIGO',
            'INDUSINDBK',
            'INDUSTOWER',
            'INFY',
            'INOXWIND',
            'IOC',
            'IREDA',
            'IRFC',
            'ITC',

            'JINDALSTEL',
            'JIOFIN',
            'JSWENERGY',
            'JSWSTEEL',
            'JUBLFOOD',

            'KALYANKJIL',
            'KAYNES',
            'KEI',
            'KFINTECH',
            'KOTAKBANK',
            'KPITTECH',

            'LAURUSLABS',
            'LICHSGFIN',
            'LICI',
            'LODHA',
            'LT',
            'LTF',
            'LTM',
            'LUPIN',

            'M&M',
            'MANAPPURAM',
            'MANKIND',
            'MARICO',
            'MARUTI',
            'MAXHEALTH',
            'MAZDOCK',
            'MCX',
            'MFSL',
            'MOTHERSON',
            'MOTILALOFS',
            'MPHASIS',
            'MUTHOOTFIN',

            'NAM-INDIA',
            'NATIONALUM',
            'NAUKRI',
            'NBCC',
            'NESTLEIND',
            'NHPC',
            'NMDC',
            'NTPC',
            'NUVAMA',
            'NYKAA',

            'OBEROIRLTY',
            'OFSS',
            'OIL',
            'ONGC',

            'PAGEIND',
            'PATANJALI',
            'PAYTM',
            'PERSISTENT',
            'PETRONET',
            'PFC',
            'PGEL',
            'PHOENIXLTD',
            'PIDILITIND',
            'PIIND',
            'PNB',
            'PNBHOUSING',
            'POLICYBZR',
            'POLYCAB',
            'POWERGRID',
            'POWERINDIA',
            'PPLPHARMA',
            'PREMIERENE',
            'PRESTIGE',

            'RBLBANK',
            'RECLTD',
            'RVNL',

            'SAIL',
            'SAMMAANCAP',
            'SBICARD',
            'SBILIFE',
            'SBIN',
            'SHREECEM',
            'SHRIRAMFIN',
            'SIEMENS',
            'SOLARINDS',
            'SONACOMS',
            'SRF',
            'SUNPHARMA',
            'SUPREMEIND',
            'SUZLON',
            'SWIGGY',
            
            'TATACONSUM',
            'TATAELXSI',
            'TATAPOWER',
            'TATASTEEL',
            'TATATECH',
            'TCS',
            'TECHM',
            'TIINDIA',
            'TMPV',
            'TITAN',
            'TORNTPHARM',
            'TORNTPOWER',
            'TRENT',
            'TVSMOTOR',

            'ULTRACEMCO',
            'UNIONBANK',
            'UNITDSPR',
            'UNOMINDA',
            'UPL',

            'VBL',
            'VEDL',
            'VMM',
            'VOLTAS',
            
            'WAAREEENER',
            'WIPRO',

            'YESBANK',

            'ZYDUSLIFE'
]


class TradeScanner:
    """Scanner for NSE F&O intraday trading signals"""

    def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=10)

    def fetch_stock_data(self, symbol: str) -> Dict:
        """Fetch 5-minute interval data for a stock"""
        try:
            data = yf.download(
                symbol + ".NS",
                period="1d",
                interval="5m",
                progress=False,
                prepost=False,
            )

            if data.empty:
                return None

            # Handle MultiIndex columns
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)

            # Drop duplicates and sort by index
            data = data[~data.index.duplicated(keep="first")]
            data = data.sort_index()

            return data

        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {e}")
            return None

    def calculate_orb(self, data: pd.DataFrame) -> Tuple[float, float]:
        """Calculate Opening Range Breakout (ORB) for first 15 minutes"""
        if data.empty or len(data) < 3:
            return None, None

        try:
            # First 15 minutes (3 candles of 5-min each)
            orb_data = data.between_time("09:15", "09:30").iloc[:3]
            orb_high = orb_data["High"].max()
            orb_low = orb_data["Low"].min()

            return float(orb_high), float(orb_low)

        except Exception as e:
            logger.error(f"Error calculating ORB: {e}")
            return None, None

    def calculate_metrics(self, data: pd.DataFrame) -> Dict:
        """Calculate required trading metrics"""
        try:
            if data.empty or len(data) < 4:
                return None

            # Current values
            current_price = float(data["Close"].iloc[-1])
            open_price = float(data["Open"].iloc[0])

            # Momentum
            momentum_pct = ((current_price - open_price) / open_price) * 100

            # Volume metrics
            current_volume = float(data["Volume"].iloc[-1])
            avg_volume = float(data["Volume"].iloc[:-1].mean())

            if avg_volume == 0:
                volume_ratio = 0
            else:
                volume_ratio = current_volume / avg_volume

            # Candle strength
            high = float(data["High"].iloc[-1])
            low = float(data["Low"].iloc[-1])
            close = float(data["Close"].iloc[-1])

            bullish_strength = (close / high) if high > 0 else 0
            bearish_strength = (close / low) if low > 0 else 0

            return {
                "current_price": current_price,
                "open_price": open_price,
                "high": high,
                "low": low,
                "close": close,
                "momentum_pct": momentum_pct,
                "current_volume": current_volume,
                "avg_volume": avg_volume,
                "volume_ratio": volume_ratio,
                "bullish_strength": bullish_strength,
                "bearish_strength": bearish_strength,
            }

        except Exception as e:
            logger.error(f"Error calculating metrics: {e}")
            return None

    def check_bullish(self, symbol: str, metrics: Dict, orb_high: float) -> Dict | None:
        """Check if stock meets BULLISH conditions"""
        try:
            if not metrics or orb_high is None:
                return None

            current_price = metrics["current_price"]
            momentum_pct = metrics["momentum_pct"]
            volume_ratio = metrics["volume_ratio"]
            bullish_strength = metrics["bullish_strength"]

            # All conditions must be met
            if (
                current_price > orb_high
                and momentum_pct > 1.0
                and volume_ratio > 1.0
                and bullish_strength >= 0.95
            ):
                stop_loss = entry * 0.995
                entry = current_price
                target = entry * 1.01

                return {
                    "symbol": symbol,
                    "entry": round(entry, 2),
                    "stop_loss": round(stop_loss, 2),
                    "target": round(target, 2),
                    "momentum": round(momentum_pct, 2),
                }

            return None

        except Exception as e:
            logger.error(f"Error checking bullish for {symbol}: {e}")
            return None

    def check_bearish(self, symbol: str, metrics: Dict, orb_low: float) -> Dict | None:
        """Check if stock meets BEARISH conditions"""
        try:
            if not metrics or orb_low is None:
                return None

            current_price = metrics["current_price"]
            momentum_pct = metrics["momentum_pct"]
            volume_ratio = metrics["volume_ratio"]
            bearish_strength = metrics["bearish_strength"]

            # All conditions must be met
            if (
                current_price < orb_low
                and momentum_pct < -1.0
                and volume_ratio > 1.0
                and bearish_strength <= 1.05
            ):
                stop_loss = entry * 1.005
                entry = current_price
                target = entry * 0.99

                return {
                    "symbol": symbol,
                    "entry": round(entry, 2),
                    "stop_loss": round(stop_loss, 2),
                    "target": round(target, 2),
                    "momentum": round(momentum_pct, 2),
                }

            return None

        except Exception as e:
            logger.error(f"Error checking bearish for {symbol}: {e}")
            return None

    def scan_stock(self, symbol: str) -> Tuple[Dict | None, Dict | None]:
        """Scan a single stock for bullish and bearish setups"""
        try:
            data = self.fetch_stock_data(symbol)
            if data is None or data.empty:
                return None, None

            orb_high, orb_low = self.calculate_orb(data)
            if orb_high is None or orb_low is None:
                return None, None

            metrics = self.calculate_metrics(data)
            if metrics is None:
                return None, None

            bullish = self.check_bullish(symbol, metrics, orb_high)
            bearish = self.check_bearish(symbol, metrics, orb_low)

            return bullish, bearish

        except Exception as e:
            logger.error(f"Error scanning {symbol}: {e}")
            return None, None

    async def scan_all_stocks(self) -> Tuple[List[Dict], List[Dict]]:
        """Scan all stocks in parallel"""
        loop = asyncio.get_event_loop()
        bullish_trades = []
        bearish_trades = []

        tasks = [
            loop.run_in_executor(self.executor, self.scan_stock, symbol)
            for symbol in NSE_FO_STOCKS
        ]

        results = await asyncio.gather(*tasks, return_exceptions=False)

        for bullish, bearish in results:
            if bullish:
                bullish_trades.append(bullish)
            if bearish:
                bearish_trades.append(bearish)

        # Sort by momentum and get top 5
        bullish_trades.sort(key=lambda x: x["momentum"], reverse=True)
        bearish_trades.sort(key=lambda x: x["momentum"])

        # If no signals found, still return best available

        if not bullish_trades:
            bullish_trades = sorted(
                [bullish for bullish, _ in results if bullish],
                key=lambda x: x["momentum"],
                reverse=True
            )

        if not bearish_trades:
            bearish_trades = sorted(
                [bearish for _, bearish in results if bearish],
                key=lambda x: x["momentum"]
            )

        return bullish_trades[:5], bearish_trades[:5]


class TelegramNotifier:
    """Handle Telegram notifications"""

    def __init__(self, bot_token: str, chat_id: str):
        if not bot_token:
            raise ValueError("BOT_TOKEN is missing")

        self.bot = Bot(token=bot_token)
        self.chat_id = chat_id

    async def send_trade_message(
        self, bullish_trades: List[Dict], bearish_trades: List[Dict]
    ):
        """Send formatted trade setup message"""
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
    """Main scheduler and bot manager"""

    def __init__(self):
        self.scanner = TradeScanner()
        self.notifier = TelegramNotifier(BOT_TOKEN, CHAT_ID)
        self.scheduler = AsyncIOScheduler(timezone=IST)

    async def scan_and_notify(self):
        """Scan stocks and send notification"""
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
        """Schedule the scanning job"""
        # Run at 09:25 AM IST, Monday to Friday
        self.scheduler.add_job(
            self.scan_and_notify,
            "cron",
            day_of_week="0-4",
            hour=9,
            minute=30,
            timezone=IST,
            id="trade_scan_0925",
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