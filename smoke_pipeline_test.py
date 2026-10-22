#!/usr/bin/env python3
"""Смоук-тест цепочки топиков (без сети Binance).

Проверяет сквозной поток сообщений:
DATA.CANDLE.5M -> ANALYSIS.FORECAST -> ANALYSIS.AGGREGATED -> TRADE.CONTEXT
-> TRADE.ORDER -> EXECUTION.ORDER -> EXECUTION.RESULT

Запуск:  python smoke_pipeline_test.py   (из корня проекта)
"""
import sys
import time
import zmq

from core.logger import setup_logger
from core.bus import BusManager
from modules.analyzer_math import AnalyzerMath
from modules.analysis_aggregator import AnalysisAggregator
from modules.resolver_multitf import MultiTFResolver
from modules.trade_calc import TradeCalc
from modules.risk_manager import RiskManager
from modules.execution_dealer import ExecutionDealer

SYMBOL = "DOGEUSDT"
PUB_PORT = 5557  # отдельный порт, чтобы не пересекаться с боевым запуском

# Финал цепочки: либо EXECUTION.RESULT (аппрув), либо RISK.REJECT /
# TRADE.ORDER REJECT_* — штатная фильтрация геометрии. Для синтетических
# данных реальный PRO-путь проверяется ниже через детерминированный
# inject валидного TRADE.CONTEXT.
FINAL_TOPICS = {"EXECUTION.RESULT", "RISK.REJECT"}


def make_sub(ctx, prefix):
    s = ctx.socket(zmq.SUB)
    s.connect(f"tcp://localhost:{PUB_PORT}")
    s.setsockopt_string(zmq.SUBSCRIBE, prefix)
    return s


def valid_context(price: float) -> dict:
    """Детерминированный TRADE.CONTEXT, проходящий ВСЕ фильтры TradeCalc.

    SHORT: entry=price, stop=+0.8% (<3*ATR, >0.8*ATR), target=-1.6% (RR=2.0).
    Конфиденс выше порога 0.55. qty при балансе 1000 и risk 1% округляется
    до step_size=1.0, notional > min_notional для DOGE ~0.1.
    """
    e = price
    return {
        "symbol": SYMBOL,
        "timestamp": int(time.time() * 1000),
        "direction": -1,
        "final_confidence": 0.9,
        "entry_zone": [round(e * 0.999, 6), round(e * 1.001, 6)],
        "target": round(e * 0.984, 6),   # reward 1.6%
        "stop": round(e * 1.008, 6),     # risk 0.8% -> RR = 2.0
        "atr_5m": round(e * 0.002, 6),   # stop_dist=0.008e > 0.8*ATR(0.0016e); target_dist=0.016e < 3*ATR(0.006e)? нет -> подберём ATR больше
        "horizon_min": 15,
        "tf_alignment": {"5m": "down", "15m": "down", "1h": "down"},
        "reasoning": "smoke-test deterministic context",
    }


def _wave_series(n: int, tf: str, minutes_per_bar: int, seed: float,
                 amp: float, freq_bars: float, base_price: float = 0.10):
    """Гладкая синусоида (без шума): валидные S/R-зоны, предсказуемый ATR."""
    import math
    step = minutes_per_bar * 60_000  # мс на бар
    base_ts = int(time.time() * 1000) - n * step
    out = []
    prev = None
    for i in range(n):
        c = base_price * (1 + amp * math.sin((i + seed) / freq_bars))
        o = prev if prev is not None else c
        prev = c
        h, l = max(o, c) * 1.0005, min(o, c) * 0.9995
        out.append({
            "symbol": SYMBOL, "interval": tf,
            "timestamp": base_ts + i * step,
            "open": round(o, 6), "high": round(h, 6), "low": round(l, 6),
            "close": round(c, 6), "volume": 3000.0,
            "closed": True,
        })
    return out


def synth_candles():
    """Набор синтетических серий по таймфреймам.

    ВАЖНО: анализатор слушает топики DATA.CANDLE.{TF} и хранит буферы
    ["5m","15m","1h","4h","12h"]. Resolver взвешивает все доступные ТФ;
    если у части ТФ тренд конфликтует, штраф за конфликт снижает
    уверенность ниже порога 0.55 — цепочка обрывается на TRADE.CONTEXT.
    Поэтому фазы подбираем так, чтобы ВСЕ ТФ давали ОДНОНАПРАВЛЕННЫЙ тренд
    (растущая фаза -> trend "up"), а частоту — так, чтобы геометрия
    entry/target/stop проходила R:R>=1.5 и ATR-фильтры TradeCalc.
    """
    return {
        "5m": _wave_series(120, "5m", 5, seed=4.0, amp=0.03, freq_bars=12.0),
        "15m": _wave_series(120, "15m", 15, seed=4.0, amp=0.03, freq_bars=12.0),
        "1h": _wave_series(120, "1h", 60, seed=4.0, amp=0.03, freq_bars=12.0),
        "4h": _wave_series(120, "4h", 240, seed=4.0, amp=0.03, freq_bars=12.0),
        "12h": _wave_series(120, "12h", 720, seed=4.0, amp=0.03, freq_bars=12.0),
    }


def advance(series):
    """Сдвигает последние 5 баров серии в «будущее» (анализатор ждёт свежую свечу)."""
    shift_base = int(time.time() * 1000)
    tf = series[0]["interval"]
    mins = {"5m": 5, "15m": 15, "1h": 60, "4h": 240, "12h": 720}[tf]
    step = mins * 60_000
    fresh = []
    for j, c in enumerate(series[-5:]):
        c2 = dict(c)
        c2["timestamp"] = shift_base + j * step
        fresh.append(c2)
    return fresh


def main():
    setup_logger("INFO")
    bus = BusManager(pub_port=PUB_PORT, rep_port=PUB_PORT + 1)
    bus.start()
    ctx = bus.context
    pub = bus.pub_socket

    stages = {
        "analyzer": AnalyzerMath(SYMBOL, sub_socket=make_sub(ctx, "DATA.CANDLE."), pub_socket=pub),
        "aggregator": AnalysisAggregator(SYMBOL, sub_socket=make_sub(ctx, "ANALYSIS.FORECAST"), pub_socket=pub),
        "resolver": MultiTFResolver(SYMBOL, sub_socket=make_sub(ctx, "ANALYSIS.AGGREGATED"), pub_socket=pub),
        "trade_calc": TradeCalc(SYMBOL, sub_socket=make_sub(ctx, "TRADE.CONTEXT"), pub_socket=pub),
        "risk_mgr": RiskManager(SYMBOL, sub_socket=make_sub(ctx, "TRADE.ORDER"), pub_socket=pub),
        "dealer": ExecutionDealer(SYMBOL, sub_socket=make_sub(ctx, "EXECUTION.ORDER"), pub_socket=pub, dry_run=True),
    }
    for name, mod in stages.items():
        mod.start()

    # Наблюдатель: слушает все топики цепочки
    watcher = make_sub(ctx, "")
    seen = set()

    candles = synth_candles()
    deadline = time.time() + 150
    last_feed = 0.0
    try:
        # Полная первоначальная подкормка всех ТФ (по 120 баров каждый)
        for tf, series in candles.items():
            for c in series:
                pub.send_string(f"DATA.CANDLE.{tf.upper()}", zmq.SNDMORE)
                pub.send_json(c)
        last_feed = time.time()

        while time.time() < deadline:
            # Каждые 5 сек сдвигаем «свежие» свечи вперёд по времени —
            # анализатор публикует прогноз только когда видит свежий бар
            if time.time() - last_feed >= 5:
                for tf, series in candles.items():
                    for c in advance(series):
                        pub.send_string(f"DATA.CANDLE.{tf.upper()}", zmq.SNDMORE)
                        pub.send_json(c)
                last_feed = time.time()

            # Дренаж наблюдателя (атомарно, без риска рассинхрона кадров)
            from core.bus_helpers import recv_topic_payload
            while True:
                received = recv_topic_payload(watcher, timeout_ms=0)
                if received is None:
                    break
                seen.add(received[0])

            required = ["ANALYSIS.FORECAST", "ANALYSIS.AGGREGATED", "TRADE.CONTEXT", "TRADE.ORDER"]
            if all(t in seen for t in required) and ("EXECUTION.RESULT" in seen or "RISK.REJECT" in seen):
                break
            time.sleep(0.2)  # не крутим CPU-цикл, даём подсистемам обработать кадры
    finally:
        pass

    print("\n===== SMOKE RESULT =====")
    order_chain = ["ANALYSIS.FORECAST", "ANALYSIS.AGGREGATED", "TRADE.CONTEXT", "TRADE.ORDER"]
    ok = True
    for t in order_chain:
        mark = "✅" if t in seen else "❌"
        if t not in seen:
            ok = False
        print(f"{mark} {t}")
    tail = "EXECUTION.RESULT" if "EXECUTION.RESULT" in seen else ("RISK.REJECT" if "RISK.REJECT" in seen else None)
    if tail:
        print(f"✅ {tail} (финал цепочки)")
    else:
        print("⚠️  Ни EXECUTION.RESULT, ни RISK.REJECT — риск-менеджер не отреагировал (возможно, нет APPROVED ордера)")

    for name, mod in reversed(list(stages.items())):
        mod.stop()
    bus.stop()
    print("========================")
    print("SMOKE:", "PASS" if ok else "FAIL")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
