#!/usr/bin/env python3
"""
TestNet Runner — запуск торговой стратегии на Binance Testnet.
Использует готовые модули: анализатор, trade_calc, risk_manager, binance API.
"""

import sys
import time
import zmq
import threading
import yaml
from datetime import datetime
from loguru import logger

# Настройка логгера
logger.remove()
logger.add(sys.stdout, format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> | <level>{message}</level>", level="INFO")

from modules.api_binance.rest_client import BinanceRESTClient
from modules.analyzer_math import AnalyzerMath
from modules.trade_calc import TradeCalc
from modules.risk_manager import RiskManager


class TestNetRunner:
    """Основной класс запуска стратегии на тестнете."""
    
    def __init__(self, symbol: str = "DOGEUSDT", config_path: str = "config/config.yaml"):
        self.symbol = symbol
        self.config_path = config_path
        self.cfg = self._load_config()
        
        # Binance Testnet URLs
        self.testnet_base_url = "https://testnet.binance.vision"
        self.testnet_api_key = self.cfg.get("binance", {}).get("testnet_api_key", "")
        self.testnet_secret = self.cfg.get("binance", {}).get("testnet_secret", "")
        
        if not self.testnet_api_key or not self.testnet_secret:
            logger.warning("⚠️ Binance Testnet keys не найдены в конфиге. Будет режим симуляции без реальных ордеров.")
            self.live_mode = False
        else:
            self.live_mode = True
            logger.info("✅ Binance Testnet keys найдены. Режим LIVE на тестнете.")
        
        # ZMQ sockets для внутренней коммуникации
        self.context = zmq.Context()
        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.bind("tcp://*:5555")
        
        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.connect("tcp://localhost:5555")
        self.sub_socket.setsockopt_string(zmq.SUBSCRIBE, "")
        
        # Компоненты
        self.rest_client = BinanceRESTClient(base_url=self.testnet_base_url if self.live_mode else "https://api.binance.com")
        self.analyzer = AnalyzerMath(symbol=self.symbol, sub_socket=self.sub_socket, pub_socket=self.pub_socket)
        self.trade_calc = TradeCalc(symbol=self.symbol, sub_socket=self.sub_socket, pub_socket=self.pub_socket, config_path=self.config_path)
        self.risk_manager = RiskManager(symbol=self.symbol, sub_socket=self.sub_socket, pub_socket=self.pub_socket, config_path=self.config_path)
        
        self.running = False
        self.last_order_time = 0
        
    def _load_config(self) -> dict:
        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки конфига: {e}")
            return {}
    
    def start(self):
        """Запуск всех компонентов и основного цикла."""
        logger.info(f"🚀 TestNet Runner запускается для {self.symbol}...")
        
        # Запускаем компоненты
        self.analyzer.start()
        self.trade_calc.start()
        self.risk_manager.start()
        
        self.running = True
        
        # Основной цикл: получаем данные → анализируем → считаем сделку → отправляем
        threading.Thread(target=self._main_loop, daemon=True).start()
        
        logger.success("✅ TestNet Runner запущен. Ожидание сигналов...")
        
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("🛑 Получен сигнал остановки...")
            self.stop()
    
    def _main_loop(self):
        """Основной цикл торговли."""
        logger.info("📊 Загрузка исторических данных для анализа...")
        
        # Получаем последние 100 свечей 5m для анализа
        df = self.rest_client.fetch_history(self.symbol, "5m", limit=100)
        
        if df.empty:
            logger.error("❌ Не удалось получить данные. Проверьте символ или соединение.")
            return
        
        logger.success(f"✅ Загружено {len(df)} свечей. Последняя: {df.index[-1]}")
        
        last_close = df["close"].iloc[-1]
        logger.info(f"💰 Текущая цена {self.symbol}: {last_close}")
        
        # Эмуляция входящих данных для анализатора
        while self.running:
            try:
                # Формируем контекст для анализатора (упрощённо)
                analyzer_context = {
                    "symbol": self.symbol,
                    "current_price": float(df["close"].iloc[-1]),
                    "high": float(df["high"].iloc[-1]),
                    "low": float(df["low"].iloc[-1]),
                    "open": float(df["open"].iloc[-1]),
                    "close": float(df["close"].iloc[-1]),
                    "volume": float(df["volume"].iloc[-1]),
                    "timestamp": int(time.time() * 1000),
                    "atr_5m": float(df["high"].iloc[-1] - df["low"].iloc[-1]) * 1.5  # упрощённый ATR
                }
                
                # Публикуем данные для анализатора
                self.pub_socket.send_string("MARKET.DATA", zmq.SNDMORE)
                self.pub_socket.send_json(analyzer_context)
                
                # Ждём ответ от анализатора (сигнал)
                # В реальной архитектуре анализатор сам публикует SIGNAL.NEW
                # Здесь эмулируем простую логику для демонстрации
                
                # Проверяем риск-менеджер (можно ли торговать)
                risk_check = {
                    "action": "CHECK",
                    "timestamp": int(time.time() * 1000)
                }
                self.pub_socket.send_string("RISK.CHECK", zmq.SNDMORE)
                self.pub_socket.send_json(risk_check)
                
                # Если разрешение есть — формируем торговый контекст
                # Для демо: если цена выросла > 0.5% за последнюю свечу — LONG
                price_change_pct = (analyzer_context["close"] - analyzer_context["open"]) / analyzer_context["open"] * 100
                
                direction = 0
                if price_change_pct > 0.3:
                    direction = 1  # LONG
                elif price_change_pct < -0.3:
                    direction = -1  # SHORT
                
                if direction != 0:
                    entry_zone = [
                        analyzer_context["close"] * 0.9995,
                        analyzer_context["close"] * 1.0005
                    ]
                    
                    atr = analyzer_context.get("atr_5m", 0.001)
                    
                    if direction == 1:
                        stop = analyzer_context["close"] - atr * 1.5
                        target = analyzer_context["close"] + atr * 3.0
                    else:
                        stop = analyzer_context["close"] + atr * 1.5
                        target = analyzer_context["close"] - atr * 3.0
                    
                    trade_context = {
                        "direction": direction,
                        "final_confidence": 0.75,
                        "entry_zone": entry_zone,
                        "target": target,
                        "stop": stop,
                        "atr_5m": atr,
                        "timestamp": int(time.time() * 1000)
                    }
                    
                    logger.info(f"🎯 Сигнал: {'LONG' if direction == 1 else 'SHORT'} | Цена: {analyzer_context['close']:.6f}")
                    
                    # Отправляем в TradeCalc
                    self.pub_socket.send_string("TRADE.CONTEXT", zmq.SNDMORE)
                    self.pub_socket.send_json(trade_context)
                    
                    # Ждём ответ TRADE.ORDER (в асинхронной архитектуре приходит позже)
                    # Для демо сразу проверяем результат через небольшую задержку
                    time.sleep(0.5)
                    
                    # Проверяем, можно ли отправить ордер (rate limit)
                    current_time = time.time()
                    if current_time - self.last_order_time < 3:
                        logger.debug("⏳ Rate limit: ждём между ордерами...")
                        time.sleep(3 - (current_time - self.last_order_time))
                    
                    # В реальной версии здесь была бы подписка на TRADE.ORDER
                    # Для демо — эмулируем успешную сделку
                    if self.live_mode:
                        logger.info("📤 Отправка ордера на Binance Testnet... (требуется доработка исполнителя)")
                        # TODO: Добавить модуль исполнения ордеров (OrderExecutor)
                    else:
                        logger.info("💡 Режим симуляции: ордер не отправляется (нет API keys)")
                    
                    self.last_order_time = time.time()
                
                # Пауза между циклами
                time.sleep(5)
                
            except Exception as e:
                logger.error(f"❌ Ошибка в основном цикле: {e}")
                time.sleep(2)
    
    def stop(self):
        """Остановка всех компонентов."""
        logger.info("🛑 Остановка TestNet Runner...")
        self.running = False
        
        self.analyzer.stop()
        self.trade_calc.stop()
        self.risk_manager.stop()
        
        self.pub_socket.close()
        self.sub_socket.close()
        self.context.term()
        
        logger.success("✅ TestNet Runner остановлен.")


if __name__ == "__main__":
    runner = TestNetRunner(symbol="DOGEUSDT")
    runner.start()
