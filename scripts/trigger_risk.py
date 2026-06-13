import asyncio
import sys
import os

sys.path.insert(0, "/app/processing/engine/src")
sys.path.insert(0, "/app")

from engine.risk_engine import RiskEngine

async def main():
    engine = RiskEngine()
    print("Starting risk scoring cycle...")
    await engine.run_scoring_cycle()
    engine._shutdown()
    print("Risk scoring cycle complete.")

if __name__ == "__main__":
    asyncio.run(main())
