import time
from modules.api_binance.ws_client import BinanceWebSocket

def on_new_candle(candle_data):
    print(f"🔥 NEW CANDLE: {candle_data['timestamp']} | Close: {candle_data['close']}")

if __name__ == "__main__":
    ws = BinanceWebSocket("DOGEUSDT", "1m", on_new_candle)
    ws.start()
    
    try:
        # Ждем 2 минуты, чтобы поймать хотя бы одну закрытую свечу
        print("⏳ Waiting for closed candles (approx 1 min)...")
        time.sleep(70) 
    except KeyboardInterrupt:
        print("🛑 Stopping...")
        ws.stop()