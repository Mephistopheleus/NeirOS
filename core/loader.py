import importlib
import os
from loguru import logger

class ModuleLoader:
    def __init__(self, modules_path: str = "modules"):
        self.modules_path = modules_path
        self.loaded_modules = []

    def load_all(self):
        logger.info("🔍 Scanning for modules...")
        if not os.path.exists(self.modules_path):
            os.makedirs(self.modules_path)
            logger.warning(f"📂 Created empty folder: {self.modules_path}")
            return []

        # Ищем все .py файлы в папке modules
        for filename in os.listdir(self.modules_path):
            if filename.endswith(".py") and filename != "__init__.py":
                module_name = filename[:-3]
                try:
                    # Динамическая загрузка
                    module = importlib.import_module(f"{self.modules_path}.{module_name}")
                    self.loaded_modules.append(module)
                    logger.info(f"✅ Loaded module: {module_name}")
                except Exception as e:
                    logger.error(f"❌ Failed to load {module_name}: {e}")
        
        return self.loaded_modules