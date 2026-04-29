from core.logger import setup_logger
from loguru import logger
from core.core import Core

if __name__ == "__main__":
    # Логгер инициализируется ДО try, чтобы except не падал
    setup_logger("INFO")
    
    try:
        logger.info("🌱 Initializing TradingBot environment...")
        app = Core()
        app.start()
    except KeyboardInterrupt:
        logger.info("👋 Graceful shutdown by user.")
    except Exception as e:
        # Правильный способ вывода traceback в Loguru
        logger.opt(exception=True).critical(f"💥 Fatal system error: {e}")
    finally:
        logger.info("🏁 System halted.")