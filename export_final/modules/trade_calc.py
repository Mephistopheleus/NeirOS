import math
import zmq
import threading
import time
import yaml
from loguru import logger

class TradeCalc:
    """
    Card 4.1: Расчёт торговой заявки.
    Вход: TRADE.CONTEXT
    Выход: TRADE.ORDER (валидная заявка или REJECT с причиной)
    """
    def __init__(self, symbol: str, sub_socket: zmq.Socket, pub_socket: zmq.Socket, config_path: str = "config/config.yaml"):
        self.symbol = symbol
        self.sub = sub_socket
        self.pub = pub_socket
        self.running = False
        self.lock = threading.Lock()
        
        self.cfg = self._load_config(config_path)
        self.risk_pct = self.cfg.get("risk_per_trade_pct", 0.01)
        self.min_rr = self.cfg.get("min_rr", 1.5)
        self.min_net_rr = self.cfg.get("min_net_rr", 1.2)
        self.atr_stop_mult = self.cfg.get("atr_stop_mult", 0.8)
        self.atr_target_mult = self.cfg.get("atr_target_mult", 3.0)
        self.fee_taker = self.cfg.get("fee_taker", 0.001)
        self.slippage_pct = self.cfg.get("slippage_pct", 0.0005)
        self.min_notional = self.cfg.get("min_notional", 5.0)
        
        # Binance DOGEUSDT rules (hardcoded for v1, later from exchange info)
        self.tick_size = 0.00001
        self.step_size = 1.0
        self.balance_usdt = self.cfg.get("initial_balance", 1000.0)

    def _load_config(self, path: str) -> dict:
        try:
            with open(path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f).get("trade_calc", {})
        except Exception:
            return {}

    def start(self):
        logger.info(f"🧮 TradeCalc starting for {self.symbol}...")
        self.running = True
        threading.Thread(target=self._listen_loop, daemon=True).start()
        logger.success("✅ TradeCalc running.")

    def _listen_loop(self):
        while self.running:
            try:
                topic = self.sub.recv_string()
                if topic == "TRADE.CONTEXT":
                    ctx = self.sub.recv_json()
                    order = self._calculate(ctx)
                    self._publish(order)
            except zmq.ZMQError as e:
                if self.running:
                    logger.warning(f"⚠️ TradeCalc ZMQ error: {e}")
                time.sleep(1)
            except Exception as e:
                if self.running:
                    logger.error(f"❌ TradeCalc loop error: {e}")
                time.sleep(0.5)

    def _calculate(self, ctx: dict) -> dict:
        direction = ctx.get("direction", 0)
        conf = ctx.get("final_confidence", 0.0)
        entry_zone = ctx.get("entry_zone", [])
        target = ctx.get("target", 0.0)
        stop = ctx.get("stop", 0.0)
        atr = ctx.get("atr_5m", 0.001) # fallback, later from analyzer
        
        base = {
            "symbol": self.symbol,
            "timestamp": int(time.time() * 1000),
            "status": "REJECT_UNKNOWN",
            "direction": direction,
            "qty": 0.0,
            "entry_price": 0.0,
            "target_price": 0.0,
            "stop_price": 0.0,
            "risk_usdt": 0.0,
            "reward_usdt": 0.0,
            "rr_ratio": 0.0,
            "net_rr": 0.0,
            "fee_usdt": 0.0,
            "slippage_usdt": 0.0,
            "breakeven": 0.0,
            "confidence": conf,
            "notional": 0.0,
            "reasoning": ""
        }

        if conf < 0.55 or direction == 0:
            base["status"] = "REJECT_LOW_CONFIDENCE"
            base["reasoning"] = f"Conf {conf:.2f} < 0.55 or flat trend"
            return base

        if len(entry_zone) != 2:
            base["status"] = "REJECT_INVALID_ENTRY"
            base["reasoning"] = "Entry zone malformed"
            return base

        entry = sum(entry_zone) / 2.0

        # 1. Геометрия
        if direction == 1:
            if not (stop < entry < target):
                base["status"] = "REJECT_GEOMETRY"
                base["reasoning"] = f"LONG invalid: stop({stop}) < entry({entry}) < target({target})"
                return base
        else:
            if not (target < entry < stop):
                base["status"] = "REJECT_GEOMETRY"
                base["reasoning"] = f"SHORT invalid: target({target}) < entry({entry}) < stop({stop})"
                return base

        stop_dist = abs(entry - stop)
        reward_dist = abs(target - entry)
        if stop_dist <= 0:
            base["status"] = "REJECT_ZERO_STOP"
            base["reasoning"] = "Stop distance <= 0"
            return base

        # 2. R:R и ATR фильтры
        rr = reward_dist / stop_dist
        if rr < self.min_rr - 1e-9:
            base["status"] = "REJECT_RR"
            base["reasoning"] = f"R:R {rr:.2f} < {self.min_rr}"
            return base
        if stop_dist < self.atr_stop_mult * atr:
            base["status"] = "REJECT_VOLATILITY"
            base["reasoning"] = f"Stop {stop_dist:.6f} < {self.atr_stop_mult}*ATR({atr:.6f})"
            return base
        if reward_dist > self.atr_target_mult * atr:
            base["status"] = "REJECT_VOLATILITY"
            base["reasoning"] = f"Target {reward_dist:.6f} > {self.atr_target_mult}*ATR({atr:.6f})"
            return base

        # 3. Расчёт лота
        risk_usdt = self.balance_usdt * self.risk_pct
        qty_raw = risk_usdt / stop_dist
        notional_raw = qty_raw * entry

        # 4. Комиссии и проскальзывание
        fee_pct = self.fee_taker * 2
        total_cost_pct = fee_pct + self.slippage_pct
        fee_usdt = notional_raw * fee_pct
        slippage_usdt = notional_raw * self.slippage_pct
        breakeven_offset = (fee_usdt + slippage_usdt) / qty_raw if qty_raw > 0 else 0
        breakeven = entry + breakeven_offset if direction == 1 else entry - breakeven_offset
        net_reward = reward_dist - breakeven_offset
        net_rr = net_reward / stop_dist if stop_dist > 0 else 0

        if net_rr < self.min_net_rr:
            base["status"] = "REJECT_NET_RR"
            base["reasoning"] = f"Net R:R {net_rr:.2f} < {self.min_net_rr} after fees/slippage"
            return base

        # 5. Округление под Binance
        qty = math.floor(qty_raw / self.step_size) * self.step_size
        entry_r = round(entry / self.tick_size) * self.tick_size
        target_r = round(target / self.tick_size) * self.tick_size
        stop_r = round(stop / self.tick_size) * self.tick_size
        notional = qty * entry_r

        if qty <= 0:
            base["status"] = "REJECT_ZERO_QTY"
            base["reasoning"] = "Rounded qty <= 0"
            return base
        if notional < self.min_notional:
            base["status"] = "REJECT_MIN_NOTIONAL"
            base["reasoning"] = f"Notional {notional:.2f} < {self.min_notional}"
            return base

        # 6. Финальный пересчёт на округлённых значениях
        final_stop_dist = abs(entry_r - stop_r)
        final_reward_dist = abs(target_r - entry_r)
        final_risk = qty * final_stop_dist
        final_reward = qty * final_reward_dist
        final_rr = final_reward_dist / final_stop_dist if final_stop_dist > 0 else 0
        final_net_rr = (final_reward_dist - breakeven_offset) / final_stop_dist if final_stop_dist > 0 else 0

        base.update({
            "status": "VALID",
            "qty": qty,
            "entry_price": entry_r,
            "target_price": target_r,
            "stop_price": stop_r,
            "risk_usdt": round(final_risk, 4),
            "reward_usdt": round(final_reward, 4),
            "rr_ratio": round(final_rr, 3),
            "net_rr": round(final_net_rr, 3),
            "fee_usdt": round(fee_usdt, 4),
            "slippage_usdt": round(slippage_usdt, 4),
            "breakeven": round(breakeven, 6),
            "notional": round(notional, 4),
            "reasoning": f"R:R {final_rr:.2f} | Net {final_net_rr:.2f} | Stop>{self.atr_stop_mult}ATR | Notional>{self.min_notional}"
        })
        return base

    def _publish(self, order: dict):
        try:
            self.pub.send_string("TRADE.ORDER", zmq.SNDMORE)
            self.pub.send_json(order)
            if order["status"] == "VALID":
                logger.info(f"✅ TRADE.ORDER VALID | Qty: {order['qty']} | Entry: {order['entry_price']} | RR: {order['rr_ratio']}")
            else:
                logger.debug(f"⛔ TRADE.ORDER {order['status']} | {order['reasoning']}")
        except Exception as e:
            logger.error(f"❌ TradeCalc publish error: {e}")

    def stop(self):
        self.running = False
        logger.info("🛑 TradeCalc stopped.")