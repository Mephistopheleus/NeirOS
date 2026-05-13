import pandas as pd
import numpy as np
import os
import time
from loguru import logger
from modules.analyzer_math import AnalyzerMath
from modules.resolver_multitf import MultiTFResolver
from modules.trade_calc import TradeCalc
from modules.risk_manager import RiskManager

class BacktestEngine:
    """
    Card 5.1: Бэктест-движок с РЕАЛЬНЫМ пайплайном.
    Прогоняет CSV через AnalyzerMath → Resolver → TradeCalc → RiskManager.
    Без ZMQ, без демо-сигналов. Детерминировано, быстро, честно.
    """
    def __init__(self, csv_path: str, config: dict = None, initial_balance: float = 1000.0):
        self.csv_path = csv_path
        self.cfg = config or {}
        self.initial_balance = initial_balance
        
        # Параметры исполнения
        self.fee_taker = self.cfg.get("fee_taker", 0.001)
        self.slippage_pct = self.cfg.get("slippage_pct", 0.0005)
        
        # Инициализация реальных модулей (в "test mode" — без ZMQ)
        self.analyzer = AnalyzerMath(
            symbol="DOGEUSDT", 
            sub_socket=None, 
            pub_socket=None, 
            data_path=os.path.dirname(csv_path)
        )
        self.resolver = MultiTFResolver(
            symbol="DOGEUSDT",
            sub_socket=None,
            pub_socket=None
        )
        self.trade_calc = TradeCalc(
            symbol="DOGEUSDT",
            sub_socket=None,
            pub_socket=None,
            config_path=None
        )
        # Переопределяем конфиги из переданного dict
        for k, v in self.cfg.items():
            if hasattr(self.trade_calc, k):
                setattr(self.trade_calc, k, v)
                
        self.risk_mgr = RiskManager(
            symbol="DOGEUSDT",
            sub_socket=None,
            pub_socket=None,
            config_path=None
        )
        for k, v in self.cfg.items():
            if hasattr(self.risk_mgr, k):
                setattr(self.risk_mgr, k, v)
        self.risk_mgr.reset_state(initial_balance)
        
        # Состояние бэктеста
        self.balance = initial_balance
        self.peak_balance = initial_balance
        self.equity_curve = [initial_balance]
        self.trades = []
        self.position = None

    def run(self) -> dict:
        """Полный прогон истории через реальный пайплайн"""
        if not os.path.exists(self.csv_path):
            raise FileNotFoundError(f"CSV not found: {self.csv_path}")
            
        df = pd.read_csv(self.csv_path, on_bad_lines='skip')
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        df.set_index("timestamp", inplace=True)
        df.sort_index(inplace=True)
        
        logger.info(f"📊 Backtest starting | Candles: {len(df)} | Balance: {self.initial_balance}")
        
        # Прогрев буферов анализатора
        for _, row in df.iterrows():
            self.analyzer.buffers["5m"].append(row.to_dict())
            
        # Основной цикл: каждая свеча — потенциальный вход
        for i in range(100, len(df)):  # 100 свечей на прогрев индикаторов
            current_candle = df.iloc[i]
            now_ts = current_candle.name.timestamp() * 1000
            
            # 1. Проверка открытой позиции
            if self.position:
                self._check_exit(current_candle, now_ts)
                if self.position:
                    self.equity_curve.append(self.balance)
                    continue
                    
            # 2. Запуск реального анализатора на текущих данных
            # Берём последние 300 свечей для расчёта
            window = df.iloc[max(0, i-300):i+1].copy()
            forecast = self._run_analyzer_pipeline(window, now_ts)
            
            if not forecast or forecast.get("overall_confidence", 0) < 55:
                continue
                
            # 3. TradeCalc: расчёт заявки
            order = self.trade_calc._calculate(forecast)
            if order.get("status") != "VALID":
                continue
                
            # 4. RiskManager: финальный фильтр
            approved = self.risk_mgr._evaluate(order)
            if approved.get("status") != "EXECUTION_APPROVED":
                continue
                
            # 5. Открытие позиции
            self._open_position(current_candle, approved, now_ts)
            self.equity_curve.append(self.balance)
            
        return self._calc_metrics()

    def _run_analyzer_pipeline(self, window: pd.DataFrame, now_ts: int) -> dict:
        """Прямой вызов AnalyzerMath + Resolver (без ZMQ)"""
        # 1. Анализ по 5m (основной ТФ для скальпинга)
        tf_result = self.analyzer._analyze_tf("5m", window)
        if not tf_result:
            return None
            
        # 2. Формируем упрощённый forecast (как если бы пришли данные по всем ТФ)
        # В реальном бэктесте можно запустить ресемпл на 15m/1h/4h здесь
        forecast = {
            "symbol": "DOGEUSDT",
            "timestamp": now_ts,
            "horizon_min": 10,
            "timeframes": {"5m": tf_result},
            "overall_confidence": tf_result["confidence"],
            "dominant_trend": tf_result["trend"],
            "key_sr_levels": [z["range"][0] for z in tf_result.get("sr_zones", [])[:2]]
        }
        
        # 3. Resolver: взвешивание (здесь один ТФ, но логика та же)
        context = self.resolver._resolve(forecast)
        return context

    def _open_position(self, candle, order, now_ts):
        """Симуляция входа с проскальзыванием и комиссией"""
        d = order["direction"]
        entry_raw = order["entry_price"]
        
        slippage = entry_raw * self.slippage_pct
        entry_price = entry_raw + slippage if d == 1 else entry_raw - slippage
        
        qty = order["qty"]
        notional = qty * entry_price
        fee = notional * self.fee_taker
        
        self.position = {
            "direction": d,
            "entry_price": entry_price,
            "target": order["target_price"],
            "stop": order["stop_price"],
            "qty": qty,
            "fee_entry": fee,
            "open_time": now_ts
        }
        self.risk_mgr.last_trade_time = now_ts / 1000
        self.risk_mgr.open_positions += 1
        self.balance -= fee

    def _check_exit(self, candle, now_ts):
        """Реалистичная проверка выхода внутри свечи"""
        pos = self.position
        d = pos["direction"]
        
        exit_price = None
        hit_type = None
        
        if d == 1:
            if candle["low"] <= pos["stop"]:
                exit_price = pos["stop"]; hit_type = "stop"
            elif candle["high"] >= pos["target"]:
                exit_price = pos["target"]; hit_type = "target"
        else:
            if candle["high"] >= pos["stop"]:
                exit_price = pos["stop"]; hit_type = "stop"
            elif candle["low"] <= pos["target"]:
                exit_price = pos["target"]; hit_type = "target"
                
        if exit_price is None:
            return
            
        slippage = exit_price * self.slippage_pct
        exit_price = exit_price - slippage if d == 1 else exit_price + slippage
        
        price_diff = (exit_price - pos["entry_price"]) if d == 1 else (pos["entry_price"] - exit_price)
        gross_pnl = pos["qty"] * price_diff
        fee_exit = pos["qty"] * exit_price * self.fee_taker
        net_pnl = gross_pnl - pos["fee_entry"] - fee_exit
        
        self.balance += net_pnl
        self.peak_balance = max(self.peak_balance, self.balance)
        is_win = net_pnl > 0
        self.risk_mgr.consecutive_losses = 0 if is_win else self.risk_mgr.consecutive_losses + 1
        self.risk_mgr.open_positions = max(0, self.risk_mgr.open_positions - 1)
        
        self.trades.append({
            "open_time": pos["open_time"],
            "close_time": now_ts,
            "direction": d,
            "entry": pos["entry_price"],
            "exit": exit_price,
            "qty": pos["qty"],
            "pnl": round(net_pnl, 4),
            "hit": hit_type,
            "balance_after": round(self.balance, 4)
        })
        self.position = None

    def _calc_metrics(self) -> dict:
        if not self.trades:
            return {"status": "NO_TRADES", "net_pnl": 0, "win_rate": 0, "max_dd_pct": 0, "profit_factor": 0}
            
        pnls = [t["pnl"] for t in self.trades]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        
        net_pnl = sum(pnls)
        win_rate = len(wins) / len(pnls)
        gross_profit = sum(wins)
        gross_loss = abs(sum(losses))
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')
        
        equity = np.array(self.equity_curve)
        running_max = np.maximum.accumulate(equity)
        dd = (running_max - equity) / running_max
        max_dd = float(np.max(dd)) if len(dd) > 0 else 0.0
        
        return {
            "status": "COMPLETED",
            "total_trades": len(self.trades),
            "net_pnl": round(net_pnl, 4),
            "win_rate": round(win_rate, 4),
            "profit_factor": round(profit_factor, 3),
            "max_dd_pct": round(max_dd, 4),
            "avg_win": round(np.mean(wins), 4) if wins else 0,
            "avg_loss": round(np.mean(losses), 4) if losses else 0,
            "final_balance": round(self.balance, 4),
            "trades": self.trades
        }

    def save_trade_log(self, path: str = "backtest_trades.csv"):
        if self.trades:
            pd.DataFrame(self.trades).to_csv(path, index=False)
            logger.info(f"📄 Trade log saved: {path}")