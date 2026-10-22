#!/usr/bin/env python3
"""
AnalysisAggregator — заглушка (stub) мета-агрегатора по плану pipeline_repair_plan.md.

Долгосрочная роль: слой, который получает ВЫХОДЫ всех анализаторов
(ANALYSIS.FORECAST.*), оценивает вес каждого анализатора по его исторической
результативности (win/loss, expectancy, влияние на просадку) и публикует
взвешенный консолидированный сигнал ANALYSIS.AGGREGATED.

Вход:  ANALYSIS.FORECAST          (пока один анализатор — AnalyzerMath)
Выход: ANALYSIS.AGGREGATED        (взвешенный прогноз для резолвера)

Сейчас это детерминированная заглушка: passthrough с нормализованными весами.
Позже будет заменена нейромоделью, обучающейся на реализованных исходах сделок.
"""

import threading
import time
import zmq
from core.bus_helpers import recv_topic_payload
from loguru import logger


class AnalysisAggregator:
    """Card 3.2 (stub): взвешивание результатов анализаторов."""

    def __init__(self, symbol: str, sub_socket: zmq.Socket, pub_socket: zmq.Socket):
        self.symbol = symbol
        self.sub = sub_socket
        self.pub = pub_socket
        self.running = False
        self.lock = threading.Lock()

        # Веса анализаторов; в будущем пересчитываются по realized outcomes.
        self.analyzer_weights = {
            "AnalyzerMath": 1.0,
        }

    def start(self):
        logger.info(f"🧮 AnalysisAggregator (stub) starting for {self.symbol}...")
        self.running = True
        threading.Thread(target=self._listen_loop, daemon=True).start()
        logger.success("✅ AnalysisAggregator running.")

    def _listen_loop(self):
        while self.running:
            try:
                received = recv_topic_payload(self.sub)
                if received is not None:
                    topic, forecast = received
                    if topic == "ANALYSIS.FORECAST":
                        self._publish(self._aggregate({"AnalyzerMath": forecast}))
            except zmq.ZMQError as e:
                if self.running:
                    logger.warning(f"⚠️ AnalysisAggregator ZMQ error: {e}")
                time.sleep(1)
            except Exception as e:
                if self.running:
                    logger.error(f"❌ AnalysisAggregator loop error: {e}")
                time.sleep(0.5)

    def _aggregate(self, analyzer_outputs: dict) -> dict:
        """Детерминированная свёртка: пока один источник — passthrough + веса.
        При добавлении новых анализаторов здесь считается средневзвешенная
        уверенность и консенсусный тренд."""
        with self.lock:
            weights = dict(self.analyzer_weights)

        total_w = sum(weights.get(name, 0.0) for name in analyzer_outputs) or 1.0
        weighted_conf = sum(
            out.get("overall_confidence", 0.0) * weights.get(name, 0.0)
            for name, out in analyzer_outputs.items()
        ) / total_w

        return {
            "symbol": self.symbol,
            "timestamp": int(time.time() * 1000),
            "source": "AnalysisAggregator/stub",
            "weights": weights,
            "overall_confidence": round(weighted_conf, 3),
            "dominant_trend": max(
                (out.get("dominant_trend", "NEUTRAL") for out in analyzer_outputs.values()),
                key=lambda t: 1,
            ),
            "timeframes": next(iter(analyzer_outputs.values()), {}).get("timeframes", {}),
            "key_sr_levels": next(iter(analyzer_outputs.values()), {}).get("key_sr_levels", []),
        }

    def _publish(self, payload: dict):
        self.pub.send_string("ANALYSIS.AGGREGATED", zmq.SNDMORE)
        self.pub.send_json(payload)
        logger.debug(
            f"📤 ANALYSIS.AGGREGATED | Conf: {payload['overall_confidence']} | "
            f"Trend: {payload['dominant_trend']}"
        )

    def stop(self):
        self.running = False
        logger.info("✅ AnalysisAggregator stopped.")
