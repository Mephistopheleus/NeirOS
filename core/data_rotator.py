import os
import pandas as pd
import threading
from loguru import logger

class DataRotator:
    def __init__(self, symbol: str, base_path: str = "data", max_size_mb: int = 500, flush_rows: int = 100):
        # max_size_mb игнорируется для простоты CSV, но оставлен для совместимости API
        self.symbol = symbol
        self.base_path = base_path
        self.buffer = []
        self.flush_threshold = flush_rows
        self.lock = threading.Lock()
        self.csv_file = os.path.join(base_path, f"{symbol}.csv")
        
        os.makedirs(base_path, exist_ok=True)
        logger.info(f"💾 DataRotator initialized for {symbol} (flush every {flush_rows} rows)")

    def push(self, candle_data: dict):
        with self.lock:
            self.buffer.append(candle_data)
            if len(self.buffer) >= self.flush_threshold:
                self.flush()

    def flush(self):
        if not self.buffer:
            return
        try:
            df = pd.DataFrame(self.buffer)
            header_mode = not os.path.exists(self.csv_file)
            df.to_csv(self.csv_file, mode='a', header=header_mode, index=False)
            self.buffer = []
            logger.debug(f"💾 Flushed to {self.csv_file}")
        except Exception as e:
            logger.error(f"❌ Write error: {e}")

    def force_close(self):
        with self.lock:
            self.flush()
        logger.info("💾 DataRotator closed.")