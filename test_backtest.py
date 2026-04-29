import os
from loguru import logger
from core.logger import setup_logger
from modules.backtest_engine import BacktestEngine

if __name__ == "__main__":
    setup_logger("INFO")
    
    csv_path = "data/DOGEUSDT.csv"
    if not os.path.exists(csv_path):
        logger.error("❌ CSV not found. Run aggregator first.")
        exit(1)
        
    config = {
        "fee_taker": 0.001,
        "slippage_pct": 0.0005,
        "min_rr": 1.5,
        "min_net_rr": 1.2,
        "atr_stop_mult": 0.8,
        "atr_target_mult": 3.0,
        "max_daily_drawdown_pct": 0.03,
        "max_consecutive_losses": 3,
        "cooldown_min": 15,
        "min_trade_interval_min": 5,
        "initial_balance": 1000.0
    }
    
    engine = BacktestEngine(csv_path, config=config, initial_balance=1000.0)
    results = engine.run()
    
    logger.info("📊 BACKTEST RESULTS:")
    for k, v in results.items():
        if k != "trades":
            logger.info(f"  {k}: {v}")
            
    engine.save_trade_log("data/backtest_trades.csv")
    logger.success("✅ Backtest finished.")