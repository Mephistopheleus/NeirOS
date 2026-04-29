import os
import sys
import time
import signal
import zmq
from loguru import logger
from core.bus import BusManager
from core.loader import ModuleLoader

class Core:
    def __init__(self):
        self.bus = BusManager(pub_port=5555, rep_port=5556)
        self.loader = ModuleLoader("modules")
        self.pipeline = {}
        self.running = False

    def start(self):
        logger.info("🚀 Starting TradingBot Core...")
        self.running = True
        
        # 1. Шина
        self.bus.start()
        
        # 2. Загрузка модулей (проверка импортов)
        self.loader.load_all()
        
        # 3. Сборка конвейера (Wiring)
        logger.info("🔗 Wiring pipeline modules...")
        ctx = self.bus.context
        
        # === SUB-сокеты для каждого этапа (создаём ВСЕ перед использованием) ===
        # Analyzer: слушает все свечи по префиксу
        sub_an = ctx.socket(zmq.SUB)
        sub_an.connect("tcp://localhost:5555")
        sub_an.setsockopt_string(zmq.SUBSCRIBE, "DATA.CANDLE.")
        
        # Resolver: слушает прогнозы анализатора
        sub_res = ctx.socket(zmq.SUB)
        sub_res.connect("tcp://localhost:5555")
        sub_res.setsockopt_string(zmq.SUBSCRIBE, "ANALYSIS.FORECAST")
        
        # TradeCalc: слушает контекст от резолвера
        sub_tc = ctx.socket(zmq.SUB)
        sub_tc.connect("tcp://localhost:5555")
        sub_tc.setsockopt_string(zmq.SUBSCRIBE, "TRADE.CONTEXT")
        
        # RiskManager: слушает ордера от TradeCalc
        sub_rm = ctx.socket(zmq.SUB)
        sub_rm.connect("tcp://localhost:5555")
        sub_rm.setsockopt_string(zmq.SUBSCRIBE, "TRADE.ORDER")
        # =====================================================================
        
        # Импорт классов (после loader они уже в sys.modules)
        from modules.aggregator import Aggregator
        from modules.analyzer_math import AnalyzerMath
        from modules.resolver_multitf import MultiTFResolver
        from modules.trade_calc import TradeCalc
        from modules.risk_manager import RiskManager
        
        # Инстанцирование (все параметры явно)
        self.pipeline["aggregator"] = Aggregator(
            symbol="DOGEUSDT",
            intervals=["5m", "15m", "1h", "4h"],
            zmq_pub_socket=self.bus.pub_socket
        )
        self.pipeline["analyzer"] = AnalyzerMath(
            symbol="DOGEUSDT",
            sub_socket=sub_an,
            pub_socket=self.bus.pub_socket
        )
        self.pipeline["resolver"] = MultiTFResolver(
            symbol="DOGEUSDT",
            sub_socket=sub_res,
            pub_socket=self.bus.pub_socket
        )
        self.pipeline["trade_calc"] = TradeCalc(
            symbol="DOGEUSDT",
            sub_socket=sub_tc,
            pub_socket=self.bus.pub_socket
        )
        self.pipeline["risk_mgr"] = RiskManager(
            symbol="DOGEUSDT",
            sub_socket=sub_rm,
            pub_socket=self.bus.pub_socket
        )
        
        # Запуск всех модулей
        for name, mod in self.pipeline.items():
            mod.start()
            logger.debug(f"✅ Pipeline stage started: {name}")
            
        logger.success(f"📦 Total modules loaded: {len(self.loader.loaded_modules)}")
        logger.success("✨ SYSTEM READY. Press Ctrl+C to stop.")
        
        # 4. Idle loop (ждём сигнала остановки)
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self._signal_handler(None, None)

    def _signal_handler(self, signum, frame):
        if self.running:
            logger.info("👋 Shutdown signal received.")
            self.stop()

    def stop(self):
        logger.info("🛑 Stopping Core...")
        self.running = False
        
        # Остановка пайплайна в обратном порядке
        for name in reversed(list(self.pipeline.keys())):
            try:
                self.pipeline[name].stop()
                logger.debug(f"✅ Stopped: {name}")
            except Exception as e:
                logger.warning(f"⚠️ Error stopping {name}: {e}")
                
        self.bus.stop()
        logger.info("🏁 Core stopped.")