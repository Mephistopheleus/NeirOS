"""Ручная проверка REST-клиента Binance (не автотест — требует сети).

Запуск:  python test_rest.py
"""
from modules.api_binance.rest_client import BinanceRESTClient


def main():
    client = BinanceRESTClient()
    # Качаем последние 2000 свечей 5m для DOGEUSDT
    df = client.fetch_history("DOGEUSDT", "5m", limit=2000)
    print(df.head())
    print(df.tail())


if __name__ == "__main__":
    main()
