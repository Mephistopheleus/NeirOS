#!/usr/bin/env python3
"""
TestNet Runner — запуск торговой стратегии на Binance Testnet.
Использует готовые модули: анализатор, trade_calc, risk_manager, binance API.
Логика: Прогноз цены + Уверенность → Вход → SL/TP на основе прогноза.
Объем: до 5% от баланса, зависит от уверенности. Фильтр по рентабельности (PnL > комиссии).
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
from modules.order_executor import OrderExecutor


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
        
        # Настройки стратегии
        self.confidence_threshold = 0.60  # Минимальная уверенность для входа
        self.balance_start = 200.0  # Выделенный баланс для работы (USDT)
        self.max_position_pct = 0.05  # Максимум 5% от баланса на сделку
        self.min_conf_for_max_size = 0.85  # При какой уверенности берём макс. объем
        self.fee_taker = self.cfg.get("trade_calc", {}).get("fee_taker", 0.001)
        self.slippage_pct = self.cfg.get("trade_calc", {}).get("slippage_pct", 0.0005)
        self.min_profit_threshold = 2.0  # Мин. прибыль в USDT, чтобы сделка имела смысл
        
        # Телеметрия сессии
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.session_stats = {
            "session_id": self.session_id,
            "start_time": datetime.now().isoformat(),
            "allocated_balance": self.balance_start,
            "current_balance": self.balance_start,
            "total_trades": 0,
            "successful_trades": 0,
            "failed_trades": 0,
            "rejected_trades": 0,
            "rejection_reasons": {},
            "open_trades": [],
            "closed_trades": [],
            "total_pnl": 0.0,
            "max_drawdown": 0.0,
            "winrate": 0.0
        }
        
        self.running = False
        self.last_order_time = 0
        self.current_forecast = None  # Последний полученный прогноз
        
        # Order Executor
        if self.live_mode:
            self.executor = OrderExecutor(
                api_key=self.testnet_api_key,
                secret_key=self.testnet_secret,
                base_url=self.testnet_base_url
            )
        else:
            self.executor = None
        
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
        
        # Цикл получения прогнозов от анализатора
        threading.Thread(target=self._forecast_listener, daemon=True).start()
        
        logger.success("✅ TestNet Runner запущен. Ожидание прогнозов...")
        
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("🛑 Получен сигнал остановки...")
            self.stop()
    
    def _forecast_listener(self):
        """Слушает прогнозы от анализатора через ZMQ."""
        logger.info("📡 Запуск слушателя прогнозов...")
        while self.running:
            try:
                # Используем poll для таймаута вместо timeout в recv_string
                if self.sub_socket.poll(1000, zmq.POLLIN):
                    topic = self.sub_socket.recv_string(zmq.NOBLOCK)
                    if topic == "ANALYSIS.FORECAST":
                        forecast = self.sub_socket.recv_json(zmq.NOBLOCK)
                        self.current_forecast = forecast
                        conf = forecast.get("overall_confidence", 0.0)
                        trend = forecast.get("dominant_trend", "unknown")
                        logger.debug(f"📊 Прогноз получен | Уверенность: {conf:.3f} | Тренд: {trend}")
            except zmq.Again:
                # Нет данных, продолжаем цикл
                continue
            except zmq.ZMQError as e:
                if self.running:
                    logger.debug(f"⚠️ ZMQ ошибка в слушателе (нормально): {e}")
                time.sleep(0.5)
            except Exception as e:
                if self.running:
                    logger.error(f"❌ Ошибка в слушателе прогнозов: {e}")
                time.sleep(1)
    
    def _calculate_position_size(self, confidence: float, current_balance: float, stop_dist_pct: float) -> tuple[float, str]:
        """
        Рассчитывает объем позиции на основе уверенности и риска.
        Возвращает кортеж: (объем, причина отказа или "OK")
        - Объем зависит от уверенности (линейно от 60% до 85%)
        - Не более 5% от баланса
        - Проверяет рентабельность: PnL > комиссии + спред
        """
        # 1. Базовый объем: линейная зависимость от уверенности
        if confidence < self.confidence_threshold:
            reason = f"Низкая уверенность: {confidence:.3f} < {self.confidence_threshold}"
            self._log_rejection(reason)
            return 0.0, reason
        
        conf_factor = (confidence - self.confidence_threshold) / (self.min_conf_for_max_size - self.confidence_threshold)
        conf_factor = min(1.0, max(0.2, conf_factor))  # От 20% до 100%
        
        max_position_usdt = current_balance * self.max_position_pct
        base_position_usdt = max_position_usdt * conf_factor
        
        # 2. Расчет комиссий и спреда
        total_cost_pct = (self.fee_taker * 2) + self.slippage_pct  # Вход + выход + спред
        fees_usdt = base_position_usdt * total_cost_pct
        
        # 3. Потенциальная прибыль (при срабатывании TP)
        potential_profit_pct = stop_dist_pct * 2.0  # R:R ~ 2:1
        potential_profit_usdt = base_position_usdt * potential_profit_pct
        
        # 4. Проверка рентабельности
        net_profit_usdt = potential_profit_usdt - fees_usdt
        
        if net_profit_usdt < self.min_profit_threshold:
            reason = f"Нерентабельно: PnL={net_profit_usdt:.2f} < {self.min_profit_threshold} USDT (комиссии: {fees_usdt:.2f})"
            self._log_rejection(reason)
            return 0.0, reason
        
        logger.debug(f"💰 Расчет объема: Уверенность={confidence:.2f} → {conf_factor*100:.0f}% | Позиция: {base_position_usdt:.2f} USDT | Net PnL: {net_profit_usdt:.2f} USDT")
        
        return base_position_usdt, "OK"
    
    def _log_rejection(self, reason: str):
        """Логирование причины отказа в сделке."""
        self.session_stats["rejected_trades"] += 1
        # Нормализуем причину для группировки
        reason_key = reason.split(":")[0].strip()
        if reason_key not in self.session_stats["rejection_reasons"]:
            self.session_stats["rejection_reasons"][reason_key] = 0
        self.session_stats["rejection_reasons"][reason_key] += 1
        logger.debug(f"🚫 Отказ в сделке #{self.session_stats['rejected_trades']}: {reason}")
    
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
        
        # Ждём первый прогноз от анализатора
        logger.info("⏳ Ожидание первого прогноза от анализатора...")
        while self.running and self.current_forecast is None:
            time.sleep(1)
        
        if not self.running:
            return
        
        logger.success("✅ Первый прогноз получен. Начинаем торговлю...")
        
        # Основной торговый цикл
        while self.running:
            try:
                # Проверяем, есть ли свежий прогноз
                if self.current_forecast is None:
                    time.sleep(1)
                    continue
                
                forecast = self.current_forecast
                conf = forecast.get("overall_confidence", 0.0)
                
                # Получаем текущую цену из последних данных
                current_price = df["close"].iloc[-1]
                
                # Обновляем данные каждые 5 секунд для свежести
                if time.time() % 5 < 1:
                    new_df = self.rest_client.fetch_history(self.symbol, "5m", limit=1)
                    if not new_df.empty:
                        current_price = new_df["close"].iloc[-1]
                        df = new_df  # обновляем DataFrame
                
                logger.debug(f"💡 Прогноз: Уверенность={conf:.3f}, Цена={current_price}")
                
                # Проверка по уверенности
                if conf < self.confidence_threshold:
                    logger.debug(f"⏸️ Уверенность {conf:.3f} ниже порога {self.confidence_threshold}, ждём...")
                    time.sleep(5)
                    continue
                
                # Определяем направление на основе прогноза
                # Анализатор возвращает predicted_price для каждого ТФ, берём среднее или доминирующее
                timeframes = forecast.get("timeframes", {})
                if not timeframes:
                    logger.warning("⚠️ Нет данных по ТФ в прогнозе, пропускаем...")
                    time.sleep(5)
                    continue
                
                # Берём прогноз с доминирующего ТФ (например, 1h или 4h)
                # Для простоты: усредняем прогнозы всех ТФ
                predicted_prices = [tf_data.get("predicted_price", current_price) for tf_data in timeframes.values()]
                avg_predicted_price = sum(predicted_prices) / len(predicted_prices)
                
                price_diff_pct = (avg_predicted_price - current_price) / current_price * 100
                
                logger.info(f"🎯 Прогноз цены: {avg_predicted_price:.6f} (текущая: {current_price:.6f}, разница: {price_diff_pct:+.2f}%)")
                
                direction = 0
                if price_diff_pct > 0.5:  # Прогноз выше текущей цены на 0.5%+
                    direction = 1  # LONG
                elif price_diff_pct < -0.5:  # Прогноз ниже текущей цены на 0.5%+
                    direction = -1  # SHORT
                
                if direction == 0:
                    logger.debug("➡️ Движение слишком маленькое, ждём...")
                    time.sleep(5)
                    continue
                
                # Рассчитываем точки входа, SL и TP
                entry_price = current_price
                stop_dist_pct = 0.02  # 2% стоп-лосс
                
                # Стоп-лосс: фиксированные 2% от входа
                if direction == 1:
                    stop_loss = entry_price * (1 - stop_dist_pct)
                    # Тейк-профит: чуть ниже прогноза (для надёжности)
                    # Если прогноз на +3%, ставим TP на +2.5%
                    tp_safety_margin = 0.005  # 0.5% запаса
                    take_profit = avg_predicted_price * (1 - tp_safety_margin)
                    # Проверяем, что TP > Entry
                    if take_profit <= entry_price:
                        take_profit = entry_price * 1.04  # Если прогноз слишком близко, ставим фиксированные 4%
                else:
                    stop_loss = entry_price * (1 + stop_dist_pct)
                    tp_safety_margin = 0.005
                    take_profit = avg_predicted_price * (1 + tp_safety_margin)
                    if take_profit >= entry_price:
                        take_profit = entry_price * 0.96
                
                # Расчет размера позиции на основе уверенности и рентабельности
                position_usdt, reject_reason = self._calculate_position_size(conf, self.balance_start, stop_dist_pct)
                
                if position_usdt <= 0:
                    logger.info(f"⏸️ Пропуск сделки: {reject_reason}")
                    time.sleep(5)
                    continue
                
                # Обновляем телеметрию
                self.session_stats["total_trades"] += 1
                
                # Конвертируем USDT в количество монет
                qty = position_usdt / entry_price
                
                logger.info(f"📋 Параметры сделки:")
                logger.info(f"   Направление: {'LONG' if direction == 1 else 'SHORT'}")
                logger.info(f"   Вход: {entry_price:.6f}")
                logger.info(f"   SL: {stop_loss:.6f} ({-stop_dist_pct*100:.1f}%)")
                logger.info(f"   TP: {take_profit:.6f} (прогноз: {avg_predicted_price:.6f})")
                logger.info(f"   Объем: {qty:.2f} {self.symbol.split('USDT')[0]} (~{position_usdt:.2f} USDT)")
                
                # Формируем контекст для TradeCalc
                atr = df["high"].iloc[-1] - df["low"].iloc[-1]  # упрощённый ATR
                
                trade_context = {
                    "direction": direction,
                    "final_confidence": conf,
                    "entry_zone": [entry_price * 0.9995, entry_price * 1.0005],
                    "target": take_profit,
                    "stop": stop_loss,
                    "atr_5m": atr,
                    "timestamp": int(time.time() * 1000)
                }
                
                # Отправляем в TradeCalc
                self.pub_socket.send_string("TRADE.CONTEXT", zmq.SNDMORE)
                self.pub_socket.send_json(trade_context)
                
                # Ждём ответ TRADE.ORDER (в асинхронной архитектуре приходит позже)
                # Для демо — небольшая задержка и проверка
                time.sleep(0.5)
                
                # Проверяем rate limit
                current_time = time.time()
                if current_time - self.last_order_time < 60:  # 1 минута между сделками
                    logger.debug("⏳ Пауза между сделками (60 сек)...")
                    time.sleep(60 - (current_time - self.last_order_time))
                
                # Исполнение ордера через OrderExecutor
                if self.executor and self.live_mode:
                    logger.info("📤 Отправка ордера на Binance Testnet...")
                    
                    # Создаем упрощенный ордер для исполнителя
                    exec_order = {
                        "symbol": self.symbol,
                        "qty": qty,
                        "direction": direction,
                        "entry_price": entry_price,
                        "stop_price": stop_loss,
                        "target_price": take_profit
                    }
                    
                    result = self.executor.execute(exec_order)
                    
                    if result.get("status") == "SUCCESS":
                        logger.success(f"✅ Ордер исполнен! Entry: {result['entry_order'].get('cummulativeQuoteQty', 'N/A')} USDT")
                        if result.get("oco_order"):
                            logger.success(f"✅ OCO установлен: ListID {result['oco_order']['orderListId']}")
                    elif result.get("status") == "PARTIAL":
                        logger.warning(f"⚠️ Позиция открыта, но OCO не установлен: {result.get('reason', '')}")
                    else:
                        logger.error(f"❌ Ошибка исполнения: {result.get('reason', 'Unknown')}")
                    
                    self.last_order_time = time.time()
                else:
                    logger.info("💡 Режим симуляции: ордер не отправляется (нет API keys или исполнитель не реализован)")
                    logger.info(f"   [SIM] BUY {qty:.2f} @ {entry_price:.6f}, SL: {stop_loss:.6f}, TP: {take_profit:.6f}")
                    self.last_order_time = time.time()
                
                # После сделки ждём 5 минут перед следующей проверкой
                logger.info("😴 Пауза 5 минут после сделки...")
                for _ in range(300):
                    if not self.running:
                        break
                    time.sleep(1)
                
            except Exception as e:
                logger.error(f"❌ Ошибка в основном цикле: {e}")
                time.sleep(2)
    
    def stop(self):
        """Остановка всех компонентов и вывод итоговой телеметрии."""
        logger.info("🛑 Остановка TestNet Runner...")
        self.running = False
        
        # Фиксируем время окончания сессии
        self.session_stats["end_time"] = datetime.now().isoformat()
        
        # Рассчитываем итоговую статистику
        if self.session_stats["total_trades"] > 0:
            self.session_stats["winrate"] = (self.session_stats["successful_trades"] / self.session_stats["total_trades"]) * 100
        
        # Выводим итоговый отчет
        self._print_session_report()
        
        self.analyzer.stop()
        self.trade_calc.stop()
        self.risk_manager.stop()
        
        self.pub_socket.close()
        self.sub_socket.close()
        self.context.term()
        
        logger.success("✅ TestNet Runner остановлен.")
    
    def _print_session_report(self):
        """Вывод полного отчета по сессии."""
        logger.info("=" * 60)
        logger.info("📊 ИТОГОВЫЙ ОТЧЕТ ПО СЕССИИ")
        logger.info("=" * 60)
        logger.info(f"ID сессии: {self.session_stats['session_id']}")
        logger.info(f"Начало: {self.session_stats['start_time']}")
        logger.info(f"Конец: {self.session_stats.get('end_time', 'N/A')}")
        logger.info(f"Выделенный баланс: {self.session_stats['allocated_balance']:.2f} USDT")
        logger.info(f"Текущий баланс: {self.session_stats['current_balance']:.2f} USDT")
        logger.info(f"Общий PnL: {self.session_stats['total_pnl']:+.2f} USDT")
        logger.info(f"Макс. просадка: {self.session_stats['max_drawdown']:.2f}%")
        logger.info("-" * 60)
        logger.info(f"Всего сделок: {self.session_stats['total_trades']}")
        logger.info(f"Успешных: {self.session_stats['successful_trades']}")
        logger.info(f"Проваленных: {self.session_stats['failed_trades']}")
        logger.info(f"Отказано: {self.session_stats['rejected_trades']}")
        logger.info(f"Winrate: {self.session_stats['winrate']:.1f}%")
        logger.info("-" * 60)
        if self.session_stats["rejection_reasons"]:
            logger.info("Причины отказов:")
            for reason, count in self.session_stats["rejection_reasons"].items():
                logger.info(f"  • {reason}: {count}")
        else:
            logger.info("Причины отказов: нет данных")
        logger.info("-" * 60)
        if self.session_stats["open_trades"]:
            logger.info(f"Открытые позиции: {len(self.session_stats['open_trades'])}")
            for trade in self.session_stats["open_trades"]:
                logger.info(f"  • {trade}")
        else:
            logger.info("Открытые позиции: нет")
        if self.session_stats["closed_trades"]:
            logger.info(f"Закрытые позиции: {len(self.session_stats['closed_trades'])}")
            for trade in self.session_stats["closed_trades"][-5:]:  # Последние 5
                logger.info(f"  • {trade}")
        logger.info("=" * 60)


if __name__ == "__main__":
    runner = TestNetRunner(symbol="DOGEUSDT")
    runner.start()
