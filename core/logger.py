import sys
import os
from loguru import logger

def setup_logger(console_level: str = "INFO"):
    """
    Инициализация логгера.
    console_level: уровень для терминала (INFO по умолчанию, чтобы не спамить)
    Файл ВСЕГДА пишет DEBUG+ для полной диагностики.
    """
    logger.remove()

    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    # 🖥️ Консоль: только указанный уровень и выше
    logger.add(
        sys.stderr,
        level=console_level,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan> - <level>{message}</level>",
        enqueue=True,
        colorize=True
    )

    # 📄 Файл: ВСЁ, включая DEBUG, с точным указанием места в коде
    # 📄 Файл: ВСЁ, включая DEBUG
    logger.add(
        os.path.join(log_dir, "bot_{time:YYYY-MM-DD}.log"),
        level="DEBUG",
        rotation="5 MB",
        retention=3,  # ✅ Было "3 files" → Loguru ждёт int или duration
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
        enqueue=True,
        encoding="utf-8",
        backtrace=True,
        diagnose=True
    )

    logger.info("📝 Logger initialized. Console: {}, File: DEBUG+".format(console_level))