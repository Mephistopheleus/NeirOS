#!/usr/bin/env python3
"""
ExecutionDealer — исполнитель (dealer) по плану pipeline_repair_plan.md.

Роль: ЕДИНСТВЕННЫЙ модуль, которому разрешено выставлять ордера на биржу.
Вход:  EXECUTION.ORDER  (одобренный риск-менеджером ордер)
Действие: вызывает OrderExecutor (Binance Testnet REST, подписанные запросы).
Выход: EXECUTION.RESULT (для будущего учёта realized outcomes в AnalysisAggregator)

Ключи берутся из config/config.yaml (секция binance). Если ключей нет или
запуск в dry-run режиме — ордер не отправляется, публикуется статус DRY_RUN.
"""

import threading
import time
import zmq
from core.bus_helpers import recv_topic_payload
from loguru import logger

from core.paths import CONFIG_PATH
from modules.order_executor import OrderExecutor


class ExecutionDealer:
    """Card 5.1: Dealer — consumes EXECUTION.ORDER and talks to Binance."""

    def __init__(self, symbol: str, sub_socket: zmq.Socket, pub_socket: zmq.Socket,
                 config_path: str = None, dry_run: bool = False):
        self.symbol = symbol
        self.sub = sub_socket
        self.pub = pub_socket
        self.running = False
        self.dry_run = dry_run
        self._lock = threading.Lock()

        # Загрузка ключей и настройка исполнителя
        api_key, secret, base_url = self._load_keys(config_path or CONFIG_PATH)
        if not api_key or not secret:
            logger.warning("⚠️ No Binance testnet keys in config — dealer runs in DRY-ONLY mode.")
            self.dry_run = True
            self.executor = None
        else:
            self.executor = OrderExecutor(api_key=api_key, secret_key=secret, base_url=base_url)

    @staticmethod
    def _load_keys(path: str) -> tuple[str, str, str]:
        try:
            import yaml
            with open(path, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f).get("binance", {})
            return (
                cfg.get("testnet_api_key", ""),
                cfg.get("testnet_secret", ""),
                cfg.get("base_url", "https://testnet.binance.vision"),
            )
        except Exception as e:
            logger.error(f"❌ Failed to load binance keys: {e}")
            return "", "", "https://testnet.binance.vision"

    def start(self):
        logger.info(f"💹 ExecutionDealer starting for {self.symbol} (dry_run={self.dry_run})...")
        self.running = True
        threading.Thread(target=self._listen_loop, daemon=True).start()
        logger.success("✅ ExecutionDealer running.")

    def _listen_loop(self):
        while self.running:
            try:
                received = recv_topic_payload(self.sub)
                if received is not None:
                    topic, order = received
                    if topic == "EXECUTION.ORDER":
                        self._handle_order(order)
            except zmq.ZMQError as e:
                if self.running:
                    logger.warning(f"⚠️ Dealer ZMQ error: {e}")
                time.sleep(1)
            except Exception as e:
                if self.running:
                    logger.error(f"❌ Dealer loop error: {e}")
                time.sleep(0.5)

    def _handle_order(self, order: dict):
        if order.get("status") != "EXECUTION_APPROVED":
            logger.debug(f"⏭️ Dealer ignoring non-approved order: {order.get('status')}")
            return

        # Одна позиция за раз + защита от дублей
        with self._lock:
            if getattr(self, "_position_open", False):
                logger.warning("🔒 Dealer: position already open, skipping new order.")
                self._publish_result(order, {"status": "SKIPPED", "reason": "Position already open"})
                return
            self._position_open = True

        if self.dry_run:
            logger.info(f"🧪 DRY-RUN order: {order}")
            self._publish_result(order, {"status": "DRY_RUN", "order": order})
            with self._lock:
                self._position_open = False
            return

        try:
            result = self.executor.execute(order)
        except Exception as e:
            logger.error(f"❌ Executor crashed: {e}")
            result = {"status": "ERROR", "reason": str(e)}

        if result.get("status") == "ERROR":
            with self._lock:
                self._position_open = False

        self._publish_result(order, result)

    def _publish_result(self, order: dict, result: dict):
        payload = {
            "symbol": order.get("symbol", self.symbol),
            "timestamp": int(time.time() * 1000),
            "direction": order.get("direction"),
            "qty": order.get("qty"),
            "entry_price": order.get("entry_price"),
            "stop_price": order.get("stop_price"),
            "target_price": order.get("target_price"),
            "result": result,
        }
        self.pub.send_string("EXECUTION.RESULT", zmq.SNDMORE)
        self.pub.send_json(payload)
        logger.info(f"📤 EXECUTION.RESULT | Status: {result.get('status')}")

    def stop(self):
        self.running = False
        logger.info("✅ ExecutionDealer stopped.")
