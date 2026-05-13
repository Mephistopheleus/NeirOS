import pandas as pd
import numpy as np
import scipy.signal as signal
import zmq
import threading
import time
import json
import os
from loguru import logger
from datetime import datetime

def _calc_ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()

def _calc_rsi(series: pd.Series, length: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1/length, min_periods=length, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/length, min_periods=length, adjust=False).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def _calc_atr(df: pd.DataFrame, length: int = 14) -> pd.Series:
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift()).abs()
    low_close = (df['low'] - df['close'].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.ewm(span=length, adjust=False).mean()


class AnalyzerMath:
    """
    Card 3.1: Математический анализатор.
    Вход: DATA.CANDLE.* из ZMQ + CSV история
    Выход: ANALYSIS.FORECAST (прогноз цены, уверенность, S/R, тренды по ТФ)
    """
    def __init__(self, symbol: str, sub_socket: zmq.Socket, pub_socket: zmq.Socket, data_path: str = "data"):
        self.symbol = symbol
        self.sub = sub_socket
        self.pub = pub_socket
        self.data_path = data_path
        self.buffers = {tf: [] for tf in ["5m", "15m", "1h", "4h", "12h"]}
        self.running = False
        self.lock = threading.Lock()
        
        # Веса для RawScore (сумма = 1.0)
        self.weights = {
            "trend": 0.25, "sr": 0.20, "pattern": 0.15,
            "vol": 0.15, "volatility": 0.10, "ma": 0.05, "rsi": 0.10
        }
        self.tf_mult = {"5m": 1.0, "15m": 1.5, "1h": 2.0, "4h": 3.0, "12h": 4.0}
        self.warmed_up = False

    def start(self):
        logger.info(f"🧠 AnalyzerMath starting for {self.symbol}...")
        self.running = True
        self._load_history()
        threading.Thread(target=self._listen_loop, daemon=True).start()
        threading.Thread(target=self._analysis_loop, daemon=True).start()
        logger.success("✅ AnalyzerMath running.")

    def _load_history(self):
        """Прогрев буферов из CSV"""
        csv_file = os.path.join(self.data_path, f"{self.symbol}.csv")
        if not os.path.exists(csv_file):
            logger.warning("⚠️ No history CSV found. Waiting for live data...")
            return
        try:
            # on_bad_lines='skip' безопасно пропускает строки-артефакты от старых тестов
            df = pd.read_csv(csv_file, on_bad_lines='skip')
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
            for _, row in df.iterrows():
                self.buffers["5m"].append(row.to_dict())
            logger.info(f"🔥 Warmed up 5m buffer with {len(df)} candles.")
        except Exception as e:
            logger.error(f"❌ History warmup failed: {e}")
            
    def _listen_loop(self):
        """Подписка на ZMQ и маршрутизация по ТФ"""
        while self.running:
            try:
                topic = self.sub.recv_string()
                msg = self.sub.recv_json()
                tf = topic.split(".")[-1].lower()
                if tf in self.buffers:
                    with self.lock:
                        self.buffers[tf].append(msg)
                        if len(self.buffers[tf]) > 300:
                            self.buffers[tf] = self.buffers[tf][-300:]
            except zmq.ZMQError as e:
                # Ошибки сокета (например, закрыт при shutdown)
                if self.running:
                    logger.warning(f"⚠️ ZMQ socket error: {e}")
                time.sleep(1)
            except Exception as e:
                if self.running:
                    logger.error(f"❌ Listen loop error: {e}")
                time.sleep(0.5)
    def _analysis_loop(self):
        """Цикл расчёта: каждую минуту анализируем все ТФ и публикуем прогноз"""
        while self.running:
            try:
                forecast = self._build_forecast()
                if forecast:
                    self._publish(forecast)
            except Exception as e:
                logger.error(f"❌ Analysis loop error: {e}")
            time.sleep(60)  # Расчёт раз в минуту

    def _build_forecast(self) -> dict:
        """Собирает мульти-ТФ анализ в единый отчёт"""
        tf_results = {}
        all_sr_zones = []
        
        for tf, buf in self.buffers.items():
            if len(buf) < 50:
                continue
            df = pd.DataFrame(buf)
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
            df.set_index("timestamp", inplace=True)
            
            res = self._analyze_tf(tf, df)
            if res:
                tf_results[tf] = res
                all_sr_zones.extend(res["sr_zones"])

        if not tf_results:
            return None

        # Конфлюэнция S/R (пересечение зон разных ТФ)
        key_levels = self._find_confluence(all_sr_zones)
        
        # Общая уверенность: средневзвешенная по ТФ с учётом мультипликатора
        total_conf = sum(r["confidence"] * self.tf_mult.get(tf, 1.0) for tf, r in tf_results.items())
        total_weight = sum(self.tf_mult.get(tf, 1.0) for tf in tf_results.keys())
        overall_conf = total_conf / total_weight if total_weight > 0 else 0.0
        
        # Доминирующий тренд
        trends = [r["trend"] for r in tf_results.values()]
        dominant_trend = max(set(trends), key=trends.count)

        return {
            "symbol": self.symbol,
            "timestamp": int(time.time() * 1000),
            "horizon_min": 10,
            "timeframes": tf_results,
            "overall_confidence": round(overall_conf, 3),
            "dominant_trend": dominant_trend,
            "key_sr_levels": key_levels
        }

    def _analyze_tf(self, tf: str, df: pd.DataFrame) -> dict:
        """Ядро расчёта для одного таймфрейма"""
        try:
            # 1. Индикаторы
            df = df.copy() # Безопасная копия
            df["ATR_14"] = _calc_atr(df, 14).values
            df["EMA_20"] = _calc_ema(df["close"], 20).values
            df["EMA_50"] = _calc_ema(df["close"], 50).values
            df["EMA_200"] = _calc_ema(df["close"], 200).values
            df["RSI_14"] = _calc_rsi(df["close"], 14).values
            atr = df["ATR_14"].iloc[-1]
            if pd.isna(atr) or atr <= 0:
                return None

            # 2. Свинги
            swings = self._find_swings(df, atr)
            
            # 3. Трендовые линии
            trend_info = self._build_trend_lines(swings, df)
            
            # 4. S/R зоны
            sr_zones = self._build_sr_zones(swings, df, tf, atr)
            
            # 5. MA/RSI фильтры
            ma_conf = self._eval_ma(df)
            rsi_conf = self._eval_rsi(df)
            
            # 6. Уверенность
            raw = (
                self.weights["trend"] * trend_info["align"] +
                self.weights["sr"] * self._eval_sr_proximity(df, sr_zones) +
                self.weights["pattern"] * trend_info["strength"] +
                self.weights["vol"] * self._eval_volume(df) +
                self.weights["volatility"] * self._eval_volatility(atr, df) +
                self.weights["ma"] * ma_conf +
                self.weights["rsi"] * rsi_conf
            )
            confidence = self._calibrate_confidence(raw)
            
            # 7. Прогноз цены
            pred_price = self._predict_price(df, trend_info, sr_zones, atr)
            
            return {
                "predicted_price": round(pred_price, 6),
                "confidence": round(confidence, 3),
                "trend": trend_info["direction"],
                "sr_zones": sr_zones[:3]
            }
        except Exception as e:
            # ✅ ИСПРАВЛЕНО: больше не глотаем ошибку молча
            logger.warning(f"⚠️ TF {tf} analysis failed: {e}")
            return None

    def _find_swings(self, df: pd.DataFrame, atr: float) -> dict:
        prom = max(0.5 * atr, 0.002 * df["close"].iloc[-1])
        dist = 5
        highs, _ = signal.find_peaks(df["high"], prominence=prom, distance=dist)
        lows, _ = signal.find_peaks(-df["low"], prominence=prom, distance=dist)
        return {
            "highs": [(df.index[i], df["high"].iloc[i], df["volume"].iloc[i]) for i in highs],
            "lows": [(df.index[i], df["low"].iloc[i], df["volume"].iloc[i]) for i in lows]
        }

    def _build_trend_lines(self, swings: dict, df: pd.DataFrame) -> dict:
        # Упрощённо: берём последние 3+ свинга одного типа, фитим линию
        direction = "flat"
        align = 0.5
        strength = 0.3
        slope = 0.0
        
        for swing_type in ["highs", "lows"]:
            pts = swings[swing_type][-4:]
            if len(pts) >= 3:
                x = np.arange(len(pts))
                y = np.array([p[1] for p in pts])
                try:
                    k, b = np.polyfit(x, y, 1)
                    slope = k
                    direction = "up" if k > 0 else "down"
                    align = min(1.0, abs(k) / (df["ATR_14"].iloc[-1] * 0.1))
                    strength = min(1.0, len(pts) / 5.0)
                    break
                except: pass
                
        return {"direction": direction, "align": align, "strength": strength, "slope": slope}

    def _build_sr_zones(self, swings: dict, df: pd.DataFrame, tf: str, atr: float) -> list:
        zones = []
        threshold = max(0.003 * df["close"].iloc[-1], 0.5 * atr)
        current_price = df["close"].iloc[-1]
        
        for s_type, pts in swings.items():
            if not pts: continue
            prices = [p[1] for p in pts]
            clusters = []
            for p in sorted(prices):
                merged = False
                for c in clusters:
                    if abs(p - np.mean(c)) <= threshold:
                        c.append(p)
                        merged = True
                        break
                if not merged:
                    clusters.append([p])
            
            for c in clusters:
                if len(c) < 2: continue
                center = np.mean(c)
                z_type = "support" if center < current_price else "resistance"
                
                touches = len(c)
                # ✅ ИСПРАВЛЕНО: idxmin() возвращает метку индекса, df.index[...] лишний и ломает тип
                closest_idx = df["close"].sub(center).abs().idxmin()
                age = (df.index[-1] - closest_idx).total_seconds() / 60
                
                decay = 100 * self.tf_mult.get(tf, 1.0)
                vol_ratio = 1.0
                score = touches * np.exp(-age/decay) * vol_ratio * self.tf_mult.get(tf, 1.0)
                
                zones.append({
                    "type": z_type,
                    "range": [round(min(c) - 0.2*atr, 6), round(max(c) + 0.2*atr, 6)],
                    "score": round(score, 2),
                    "touches": touches
                })
        return sorted(zones, key=lambda x: x["score"], reverse=True)

    def _eval_ma(self, df: pd.DataFrame) -> float:
        c = df["close"].iloc[-1]
        e20 = df["EMA_20"].iloc[-1]
        e50 = df["EMA_50"].iloc[-1]
        e200 = df["EMA_200"].iloc[-1]
        if pd.isna([e20, e50, e200]).any(): return 0.5
        # Конфлюэнция: цена выше всех EMA → bull, ниже всех → bear
        if c > e20 > e50 > e200: return 0.9
        if c < e20 < e50 < e200: return 0.9
        if abs(c - e50) / c < 0.003: return 0.7 # Близко к динамическому S/R
        return 0.5

    def _eval_rsi(self, df: pd.DataFrame) -> float:
        rsi = df["RSI_14"].iloc[-1]
        if pd.isna(rsi): return 0.5
        if 40 <= rsi <= 60: return 0.8 # Здоровый диапазон
        if rsi > 70 or rsi < 30: return 0.4 # Экстремум, риск отката
        return 0.6

    def _eval_sr_proximity(self, df: pd.DataFrame, zones: list) -> float:
        c = df["close"].iloc[-1]
        if not zones: return 0.5
        nearest = min(zones, key=lambda z: abs(np.mean(z["range"]) - c))
        dist = abs(np.mean(nearest["range"]) - c) / c
        return max(0.2, min(1.0, 1.0 - dist * 100)) # Чем дальше от сильного уровня, тем выше уверенность в продолжении

    def _eval_volume(self, df: pd.DataFrame) -> float:
        vol = df["volume"].iloc[-1]
        mean_vol = df["volume"].rolling(20).mean().iloc[-1]
        if pd.isna(mean_vol) or mean_vol == 0: return 0.5
        ratio = vol / mean_vol
        return min(1.0, ratio / 1.5) # >1.5x среднего → 1.0

    def _eval_volatility(self, atr: float, df: pd.DataFrame) -> float:
        mean_atr = df["ATR_14"].rolling(50).mean().iloc[-1]
        if pd.isna(mean_atr) or mean_atr == 0: return 0.5
        ratio = atr / mean_atr
        if 0.8 <= ratio <= 1.2: return 0.9 # Норма
        if ratio < 0.5 or ratio > 2.0: return 0.3 # Сжатие или аномальное расширение
        return 0.6

    def _calibrate_confidence(self, raw: float) -> float:
        # TODO: Заменить на IsotonicRegression после накопления данных бэктеста
        # Сейчас: логистическая калибровка для перевода raw в 0.0-1.0
        return float(1 / (1 + np.exp(-10 * (raw - 0.5))))

    def _predict_price(self, df: pd.DataFrame, trend: dict, zones: list, atr: float) -> float:
        c = df["close"].iloc[-1]
        horizon = 2 # 2 свечи 5m = 10 мин
        pred = c + trend["slope"] * horizon
        
        # Коррекция по S/R
        for z in zones:
            z_mid = np.mean(z["range"])
            if z["type"] == "resistance" and pred > z_mid:
                pred = z_mid - 0.1 * atr
            elif z["type"] == "support" and pred < z_mid:
                pred = z_mid + 0.1 * atr
                
        # Ограничение волатильностью
        max_move = 1.5 * atr
        pred = max(c - max_move, min(c + max_move, pred))
        return pred

    def _find_confluence(self, zones: list) -> list:
        if not zones: return []
        # Простое пересечение: если центры зон разных ТФ в пределах 0.4% → key level
        centers = [np.mean(z["range"]) for z in zones]
        keys = []
        for c in centers:
            if sum(1 for x in centers if abs(x - c)/c < 0.004) >= 2:
                keys.append(round(c, 6))
        return sorted(list(set(keys)))[:3]

    def _publish(self, forecast: dict):
        try:
            self.pub.send_string("ANALYSIS.FORECAST", zmq.SNDMORE)
            self.pub.send_json(forecast)
            logger.debug(f"📊 Published forecast | Conf: {forecast['overall_confidence']} | Trend: {forecast['dominant_trend']}")
        except Exception as e:
            logger.error(f"❌ Publish error: {e}")

    def stop(self):
        self.running = False
        logger.info("🛑 AnalyzerMath stopped.")