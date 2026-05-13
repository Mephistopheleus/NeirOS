import requests
import pandas as pd
import time
from typing import Optional
from loguru import logger

class BinanceRESTClient:
    def __init__(self, base_url: str = "https://api.binance.com", rate_limit_delay: float = 0.2):
        self.base_url = base_url
        self.rate_limit_delay = rate_limit_delay
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    def fetch_history(self, symbol: str, interval: str, limit: int = 1000,
                      start_time: Optional[int] = None, end_time: Optional[int] = None) -> pd.DataFrame:
        endpoint = "/api/v3/klines"
        params = {"symbol": symbol.upper(), "interval": interval, "limit": 1000}
        if start_time: params["startTime"] = start_time
        if end_time: params["endTime"] = end_time

        all_raw_data = []
        current_start = start_time
        fetched_count = 0

        logger.info(f"📥 Fetching history for {symbol} {interval} (target: {limit})...")

        while fetched_count < limit:
            if current_start: params["startTime"] = current_start
            try:
                response = self.session.get(f"{self.base_url}{endpoint}", params=params, timeout=10)
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    logger.warning(f"⚠️ Rate Limit (429). Sleeping {retry_after}s...")
                    time.sleep(retry_after)
                    continue
                response.raise_for_status()
                batch = response.json()
                if not batch: break

                all_raw_data.extend(batch)
                fetched_count += len(batch)
                current_start = batch[-1][0] + 1
                time.sleep(self.rate_limit_delay)
            except requests.exceptions.RequestException as e:
                logger.error(f"❌ Network error: {e}")
                time.sleep(5)
                continue
        
        all_raw_data = all_raw_data[:limit]
        if not all_raw_data: return pd.DataFrame()
        
        df = self._parse_to_dataframe(all_raw_data)
        logger.success(f"✅ History loaded: {len(df)} candles")
        return df

    def _parse_to_dataframe(self, raw_data: list) -> pd.DataFrame:
        df = pd.DataFrame(raw_data, columns=[
            "timestamp", "open", "high", "low", "close", "volume",
            "close_time", "quote_volume", "trades", "taker_buy_base",
            "taker_buy_quote", "ignore"
        ])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        df.set_index("timestamp", inplace=True)
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype(float)
        return df