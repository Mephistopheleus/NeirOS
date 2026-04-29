import time
from core.data_rotator import DataRotator

def run_test():
    rotator = DataRotator("TESTCOIN", max_size_mb=1, flush_rows=10)
    print("🚀 Sending data...")
    
    for i in range(50):
        rotator.push({
            "timestamp": int(time.time() * 1000) + i,
            "open": 100.0, "high": 105.0, "low": 95.0, "close": 102.0, "volume": 500.0
        })
        time.sleep(0.05)
        
    rotator.force_close()
    print("✅ Done! Check data/TESTCOIN.csv")

if __name__ == "__main__":
    run_test()