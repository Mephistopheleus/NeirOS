import zmq
import threading
import time
import numpy as np
from loguru import logger

class MultiTFResolver:
    """
    Card 3.3: Мульти-ТФ резолвер.
    Вход: ANALYSIS.FORECAST (содержит прогнозы по всем ТФ)
    Выход: TRADE.CONTEXT (взвешенный вердикт, entry/target/stop, уверенность)
    """
    def __init__(self, symbol: str, sub_socket: zmq.Socket, pub_socket: zmq.Socket):
        self.symbol = symbol
        self.sub = sub_socket
        self.pub = pub_socket
        self.running = False
        self.lock = threading.Lock()
        self.latest_forecast = None
        
        # Веса ТФ для скальпинга (5-30 мин горизонт)
        self.tf_weights = {
            "5m": 1.8, "15m": 2.0, "1h": 1.2, "4h": 0.8, "12h": 0.5
        }
        self.min_confidence = 0.55
        self.max_age_ms = 300_000  # 5 мин свежесть

    def start(self):
        logger.info(f"⚖️ MultiTFResolver starting for {self.symbol}...")
        self.running = True
        threading.Thread(target=self._listen_loop, daemon=True).start()
        threading.Thread(target=self._resolve_loop, daemon=True).start()
        logger.success("✅ MultiTFResolver running.")

    def _listen_loop(self):
        while self.running:
            try:
                topic = self.sub.recv_string()
                if topic == "ANALYSIS.FORECAST":
                    msg = self.sub.recv_json()
                    with self.lock:
                        self.latest_forecast = msg
            except zmq.ZMQError as e:
                if self.running:
                    logger.warning(f"⚠️ Resolver ZMQ error: {e}")
                time.sleep(1)
            except Exception as e:
                if self.running:
                    logger.error(f"❌ Resolver listen error: {e}")
                time.sleep(0.5)

    def _resolve_loop(self):
        while self.running:
            try:
                with self.lock:
                    forecast = self.latest_forecast
                
                if forecast:
                    age = time.time() * 1000 - forecast.get("timestamp", 0)
                    if age < self.max_age_ms:
                        context = self._resolve(forecast)
                        if context:
                            self._publish(context)
            except Exception as e:
                logger.error(f"❌ Resolver loop error: {e}")
            time.sleep(30)  # Проверка каждые 30 сек

    def _resolve(self, forecast: dict) -> dict:
        tfs = forecast.get("timeframes", {})
        if not tfs:
            return None

        # 1. Фильтр свежести и доступных ТФ
        valid_tfs = {tf: data for tf, data in tfs.items() if tf in self.tf_weights}
        if not valid_tfs:
            return None

        # 2. Численное направление и веса
        dir_map = {"up": 1.0, "down": -1.0, "flat": 0.0}
        weights, directions, confidences = [], [], []
        tf_alignment = {}

        for tf, data in valid_tfs.items():
            w = self.tf_weights[tf]
            d = dir_map.get(data.get("trend", "flat"), 0.0)
            c = data.get("confidence", 0.0)
            weights.append(w)
            directions.append(d)
            confidences.append(c)
            tf_alignment[tf] = data.get("trend", "flat")

        weights = np.array(weights)
        directions = np.array(directions)
        confidences = np.array(confidences)

        # 3. Взвешенное направление и уверенность
        w_sum = weights.sum()
        if w_sum == 0:
            return None

        weighted_dir = np.dot(weights, directions) / w_sum
        weighted_conf = np.dot(weights, confidences) / w_sum

        # 4. Штраф за конфликт ТФ (стандартное отклонение направлений)
        conflict_penalty = 1.0 - (np.std(directions) * 0.6)
        final_conf = max(0.0, min(1.0, weighted_conf * conflict_penalty))

        # 5. Итоговое направление
        direction = 1 if weighted_dir > 0.3 else (-1 if weighted_dir < -0.3 else 0)

        # 6. Фильтр минимальной уверенности
        if final_conf < self.min_confidence or direction == 0:
            return None  # Нет setup'а

        # 7. Расчёт entry/target/stop на основе 5m/15m + S/R
        entry, target, stop = self._calc_levels(valid_tfs, direction)

        # 8. Формирование контекста
        reasoning = self._build_reasoning(tf_alignment, final_conf, conflict_penalty)
        
        return {
            "symbol": self.symbol,
            "timestamp": int(time.time() * 1000),
            "direction": direction,
            "final_confidence": round(final_conf, 3),
            "entry_zone": [round(entry * 0.999, 6), round(entry * 1.001, 6)],
            "target": round(target, 6),
            "stop": round(stop, 6),
            "horizon_min": 15,
            "tf_alignment": tf_alignment,
            "reasoning": reasoning
        }

    def _calc_levels(self, tfs: dict, direction: int) -> tuple:
        # Приоритет 5m/15m для скальпинга
        ref_tf = "5m" if "5m" in tfs else ("15m" if "15m" in tfs else list(tfs.keys())[0])
        ref = tfs[ref_tf]
        pred = ref.get("predicted_price", 0.0)
        zones = ref.get("sr_zones", [])
        
        entry = pred
        if direction == 1:  # LONG
            # Target: ближайшее сопротивление или pred + 1.5%
            res_zones = [z for z in zones if z["type"] == "resistance"]
            target = min(res_zones[0]["range"][1], pred * 1.015) if res_zones else pred * 1.015
            # Stop: ближайшая поддержка или pred - 1.0%
            sup_zones = [z for z in zones if z["type"] == "support"]
            stop = max(sup_zones[0]["range"][0], pred * 0.990) if sup_zones else pred * 0.990
        else:  # SHORT
            sup_zones = [z for z in zones if z["type"] == "support"]
            target = max(sup_zones[0]["range"][0], pred * 0.985) if sup_zones else pred * 0.985
            res_zones = [z for z in zones if z["type"] == "resistance"]
            stop = min(res_zones[0]["range"][1], pred * 1.010) if res_zones else pred * 1.010
            
        return entry, target, stop

    def _build_reasoning(self, alignment: dict, conf: float, penalty: float) -> str:
        trends = list(alignment.values())
        dominant = max(set(trends), key=trends.count)
        conflict = "Low" if penalty > 0.8 else ("Medium" if penalty > 0.5 else "High")
        return (f"Dominant {dominant} trend. TF conflict: {conflict}. "
                f"Weighted confidence: {conf:.2f}. Scalping horizon 5-30m.")

    def _publish(self, context: dict):
        try:
            self.pub.send_string("TRADE.CONTEXT", zmq.SNDMORE)
            self.pub.send_json(context)
            logger.info(f"📡 Published TRADE.CONTEXT | Dir: {context['direction']} | Conf: {context['final_confidence']}")
        except Exception as e:
            logger.error(f"❌ Resolver publish error: {e}")

    def stop(self):
        self.running = False
        logger.info("🛑 MultiTFResolver stopped.")