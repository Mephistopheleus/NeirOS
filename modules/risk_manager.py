import zmq
import threading
import time
import yaml
from loguru import logger

class RiskManager:
    """
    Card 4.2: Риск-менеджер.
    Вход: TRADE.ORDER
    Выход: EXECUTION.ORDER (одобрено) или RISK.REJECT (отклонено)
    Ведёт состояние сессии: баланс, просадка, серия убытков, кулдауны.
    """
    def __init__(self, symbol: str, sub_socket: zmq.Socket, pub_socket: zmq.Socket, config_path: str = "config/config.yaml"):
        self.symbol = symbol
        self.sub = sub_socket
        self.pub = pub_socket
        self.running = False
        self.lock = threading.Lock()
        self.cfg = self._load_config(config_path)

        # Состояние сессии
        self.balance = self.cfg.get("initial_balance", 1000.0)
        self.peak_balance = self.balance
        self.open_positions = 0
        self.consecutive_losses = 0
        self.last_trade_time = 0
        self.cooldown_until = 0
        self.session_start_balance = self.balance

        # Правила
        self.max_open = self.cfg.get("max_open_positions", 1)
        self.max_dd_pct = self.cfg.get("max_daily_drawdown_pct", 0.03)
        self.max_cons_losses = self.cfg.get("max_consecutive_losses", 3)
        self.cooldown_min = self.cfg.get("cooldown_min", 15)
        self.min_interval_min = self.cfg.get("min_trade_interval_min", 5)
        self.min_balance = self.cfg.get("min_balance_usdt", 100.0)

    def _load_config(self, path: str) -> dict:
        try:
            with open(path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f).get("risk_manager", {})
        except Exception:
            return {}

    def start(self):
        logger.info(f"🛡️ RiskManager starting for {self.symbol}...")
        self.running = True
        threading.Thread(target=self._listen_loop, daemon=True).start()
        logger.success("✅ RiskManager running.")

    def _listen_loop(self):
        while self.running:
            try:
                topic = self.sub.recv_string()
                if topic == "TRADE.ORDER":
                    order = self.sub.recv_json()
                    result = self._evaluate(order)
                    self._publish(result)
            except zmq.ZMQError as e:
                if self.running:
                    logger.warning(f"⚠️ RiskManager ZMQ error: {e}")
                time.sleep(1)
            except Exception as e:
                if self.running:
                    logger.error(f"❌ RiskManager loop error: {e}")
                time.sleep(0.5)

    def _evaluate(self, order: dict) -> dict:
        if order.get("status") != "VALID":
            order["status"] = "RISK_REJECT_INVALID_INPUT"
            order["reasoning"] = "Input order not VALID"
            return order

        with self.lock:
            now = time.time()

            # 1. Кулдаун после серии убытков или ручной блокировки
            if now < self.cooldown_until:
                remaining = int((self.cooldown_until - now) / 60)
                order["status"] = "RISK_REJECT_COOLDOWN"
                order["reasoning"] = f"Cooldown active: {remaining}m left"
                return order

            # 2. Минимальный интервал между сделками
            if now - self.last_trade_time < self.min_interval_min * 60:
                order["status"] = "RISK_REJECT_INTERVAL"
                order["reasoning"] = f"Min interval {self.min_interval_min}m not met"
                return order

            # 3. Лимит открытых позиций
            if self.open_positions >= self.max_open:
                order["status"] = "RISK_REJECT_MAX_POSITIONS"
                order["reasoning"] = f"Open positions {self.open_positions} >= {self.max_open}"
                return order

            # 4. Просадка сессии
            dd = (self.peak_balance - self.balance) / self.peak_balance if self.peak_balance > 0 else 0
            if dd > self.max_dd_pct:
                order["status"] = "RISK_REJECT_DRAWDOWN"
                order["reasoning"] = f"Session DD {dd:.2%} > {self.max_dd_pct:.2%}"
                return order

            # 5. Серия убытков
            if self.consecutive_losses >= self.max_cons_losses:
                self.cooldown_until = now + self.cooldown_min * 60
                self.consecutive_losses = 0
                order["status"] = "RISK_REJECT_CONSECUTIVE_LOSSES"
                order["reasoning"] = f"{self.max_cons_losses} losses → cooldown {self.cooldown_min}m"
                return order

            # 6. Минимальный баланс
            if self.balance < self.min_balance:
                order["status"] = "RISK_REJECT_LOW_BALANCE"
                order["reasoning"] = f"Balance {self.balance:.2f} < {self.min_balance}"
                return order

            # ✅ Все фильтры пройдены
            order["status"] = "EXECUTION_APPROVED"
            order["reasoning"] = "Risk checks passed"
            self.last_trade_time = now
            self.open_positions += 1
            return order

    def record_close(self, pnl_usdt: float, is_win: bool):
        """Вызывается Backtest/Execution при закрытии позиции"""
        with self.lock:
            self.balance += pnl_usdt
            self.peak_balance = max(self.peak_balance, self.balance)
            self.open_positions = max(0, self.open_positions - 1)
            if is_win:
                self.consecutive_losses = 0
            else:
                self.consecutive_losses += 1
            logger.debug(f"📊 Risk state updated | Bal: {self.balance:.2f} | PnL: {pnl_usdt:+.2f} | ConsLoss: {self.consecutive_losses}")

    def reset_state(self, balance: float = None):
        """Сброс для новой итерации бэктеста"""
        with self.lock:
            self.balance = balance or self.cfg.get("initial_balance", 1000.0)
            self.peak_balance = self.balance
            self.session_start_balance = self.balance
            self.open_positions = 0
            self.consecutive_losses = 0
            self.last_trade_time = 0
            self.cooldown_until = 0
            logger.info("🔄 RiskManager state reset.")

    def _publish(self, order: dict):
        try:
            topic = "EXECUTION.ORDER" if order["status"] == "EXECUTION_APPROVED" else "RISK.REJECT"
            self.pub.send_string(topic, zmq.SNDMORE)
            self.pub.send_json(order)
            if order["status"] == "EXECUTION_APPROVED":
                logger.info(f"✅ RISK APPROVED | Qty: {order['qty']} | Entry: {order['entry_price']}")
            else:
                logger.debug(f"⛔ RISK REJECT | {order['status']} | {order['reasoning']}")
        except Exception as e:
            logger.error(f"❌ RiskManager publish error: {e}")

    def stop(self):
        self.running = False
        logger.info("🛑 RiskManager stopped.")