"""
GeoGuard Risk Score Engine — Main Orchestrator.

Reads unified data from InfluxDB + PostgreSQL every 15 minutes,
computes risk scores per zone using the SWI model, writes results
back to both databases, and publishes risk updates via MQTT for
real-time dashboard consumption.
"""
import os
import sys
import json
import asyncio
import logging
from datetime import datetime, timezone, timedelta

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from processing.engine.src.data_reader.aggregator import DataAggregator
from processing.engine.src.engine.scorer import RiskScorer
from processing.engine.src.writer.influx_writer import RiskInfluxWriter
from processing.engine.src.writer.postgis_writer import RiskPostGISWriter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [RISK-ENGINE] %(message)s",
)
logger = logging.getLogger("risk-engine")

MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_RISK_TOPIC = os.getenv("MQTT_RISK_TOPIC", "geoguard/risk/scores")


class RiskEngine:
    """
    Main orchestrator for the Risk Score Engine.

    Cycle (every 15 minutes):
    1. Aggregate data for all zones (InfluxDB + PostgreSQL)
    2. Compute risk scores per zone (SWI model)
    3. Write risk scores to InfluxDB (time-series history)
    4. Write risk scores to PostgreSQL (current state + history log)
    5. Publish risk scores via MQTT (real-time dashboard push)
    """

    def __init__(self):
        self.aggregator = DataAggregator()
        self.scorer = RiskScorer()
        self.influx_writer = RiskInfluxWriter()
        self.postgis_writer = RiskPostGISWriter()
        self.postgis_writer.connect()
        self.scheduler = AsyncIOScheduler()
        self.mqtt_client = None

    def _init_mqtt(self):
        """Initialize MQTT client for risk score publishing."""
        try:
            import mqtt
            self.mqtt_client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                client_id="geoguard-risk-engine",
            )
            self.mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
            self.mqtt_client.loop_start()
            logger.info(f"MQTT client connected to {MQTT_HOST}:{MQTT_PORT}")
        except ImportError:
            logger.warning("MQTT library not available — risk scores will not be published via MQTT")
        except Exception as e:
            logger.warning(f"MQTT connection failed: {e} — risk scores will not be published via MQTT")

    async def run_scoring_cycle(self):
        """
        Execute one complete scoring cycle for all zones.

        This is the main loop body, called every 15 minutes.
        """
        logger.info("Starting risk scoring cycle...")

        # ── 1. Aggregate data for all zones ──────────────────
        try:
            inputs = self.aggregator.aggregate_all_zones()
        except Exception as e:
            logger.error(f"Data aggregation failed: {e}")
            return

        if not inputs:
            logger.warning("No zone data available — skipping scoring cycle")
            return

        logger.info(f"Aggregated data for {len(inputs)} zones")

        # ── 2. Compute risk scores ───────────────────────────
        outputs = []
        for input in inputs:
            try:
                output = self.scorer.score_zone(input)
                outputs.append(output)
            except Exception as e:
                logger.error(f"Scoring failed for zone {input.zone_id}: {e}")

        if not outputs:
            logger.warning("No risk scores computed — skipping write cycle")
            return

        # ── 3. Write to InfluxDB (time-series history) ───────
        try:
            self.influx_writer.write_batch(outputs)
            logger.info(f"Risk scores written to InfluxDB: {len(outputs)} zones")
        except Exception as e:
            logger.error(f"InfluxDB write batch failed: {e}")

        # ── 4. Write to PostgreSQL (current state + history) ──
        try:
            self.postgis_writer.write_batch(outputs)
            logger.info(f"Risk scores written to PostgreSQL: {len(outputs)} zones")
        except Exception as e:
            logger.error(f"PostgreSQL write batch failed: {e}")

        # ── 5. Publish via MQTT (real-time push) ──────────────
        if self.mqtt_client:
            try:
                for output in outputs:
                    payload = json.dumps(output.to_dict())
                    self.mqtt_client.publish(
                        MQTT_RISK_TOPIC,
                        payload,
                        qos=1,
                    )
                logger.info(f"Risk scores published via MQTT: {len(outputs)} zones")
            except Exception as e:
                logger.error(f"MQTT publish failed: {e}")

        # ── 6. Log summary ───────────────────────────────────
        high_risk_zones = [
            o for o in outputs if o.risk_level in ("HIGH", "CRITICAL")
        ]
        logger.info(
            f"Scoring cycle complete. {len(outputs)} zones scored. "
            f"{len(high_risk_zones)} zones at HIGH+ risk: "
            f"{[z.zone_id for z in high_risk_zones]}"
        )

        # ── 7. Check for immediate alert triggers ────────────
        for output in high_risk_zones:
            logger.warning(
                f"ALERT: Zone {output.zone_id} — "
                f"risk_score={output.risk_score:.1f} ({output.risk_level}), "
                f"driver={output.primary_driver}"
            )

    def start(self):
        """Start the Risk Engine scheduler and begin processing cycles."""
        self._init_mqtt()

        # ── Schedule scoring cycle every 15 minutes ──────────
        self.scheduler.add_job(
            self.run_scoring_cycle,
            "interval",
            minutes=15,
            id="risk_scoring",
            next_run_time=datetime.now(timezone.utc) + timedelta(seconds=10),
        )

        self.scheduler.start()
        logger.info(
            "Risk Score Engine started. "
            "Scoring interval: 15 minutes. "
            f"MQTT topic: {MQTT_RISK_TOPIC}."
        )

        try:
            asyncio.get_event_loop().run_forever()
        except (KeyboardInterrupt, SystemExit):
            logger.info("Risk Score Engine shutting down.")
            self._shutdown()

    def _shutdown(self):
        """Clean shutdown of all connections and scheduler."""
        if self.scheduler.running:
            self.scheduler.shutdown()
        self.aggregator.close()
        self.scorer.close()
        self.influx_writer.close()
        self.postgis_writer.close()
        if self.mqtt_client:
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()


# ── Entry Point ────────────────────────────────────────────────
if __name__ == "__main__":
    engine = RiskEngine()
    engine.start()
