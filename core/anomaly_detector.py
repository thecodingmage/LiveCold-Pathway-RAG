"""
LiveCold — Multi-Layer Anomaly Detection Engine
Filters sensor glitches before they trigger false diversions.

4-Layer Detection:
  L1  Physical Bounds     — Reject impossible temperatures
  L2  Rate-of-Change      — Flag sudden jumps (sensor fault)
  L3  Statistical Z-Score — Detect outliers from rolling mean
  L4  Stuck Sensor        — Flag repeated identical readings
"""

import time
import threading
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional


# ══════════════════════════════════════════════════════════════════
#  Configuration — product-specific physical limits
# ══════════════════════════════════════════════════════════════════

PHYSICAL_LIMITS = {
    # product_type: (absolute_min_°C, absolute_max_°C)
    # Even in worst malfunction, values outside these are hardware errors
    "vaccines":    (-90,  30),
    "frozen_meat": (-45,  25),
    "dairy":       (-10,  35),
    "seafood":     (-30,  30),
    "pharmaceuticals": (-15, 40),
    "ice_cream":   (-50,  20),
    "fruits":      (-5,   45),
    "flowers":     (-5,   40),
    "default":     (-60,  60),
}

# Maximum physically possible rate of change (°C per second)
# Even with doors wide open, internal temp rises ~0.05°C/sec max
MAX_RATE_OF_CHANGE = 3.0    # °C per reading interval (~2 seconds)

# Z-score threshold — readings this many std devs from mean are flagged
ZSCORE_THRESHOLD = 3.5

# Stuck sensor: if N consecutive readings are identical (within epsilon)
STUCK_SENSOR_COUNT = 10
STUCK_EPSILON = 0.001       # readings within this are "identical"

# Rolling window size for statistical calculations
WINDOW_SIZE = 20


# ══════════════════════════════════════════════════════════════════
#  Anomaly Result
# ══════════════════════════════════════════════════════════════════

@dataclass
class AnomalyResult:
    """Result of anomaly detection for one reading."""
    is_anomaly: bool = False
    layer: str = ""               # "L1_BOUNDS", "L2_RATE", "L3_ZSCORE", "L4_STUCK"
    reason: str = ""
    original_temp: float = 0.0
    corrected_temp: Optional[float] = None  # fallback safe value

    def __bool__(self):
        return self.is_anomaly


# ══════════════════════════════════════════════════════════════════
#  Per-Shipment State Tracker
# ══════════════════════════════════════════════════════════════════

@dataclass
class ShipmentSensorState:
    """Maintains rolling state for one shipment's sensor readings."""
    history: list = field(default_factory=list)    # last N temps
    timestamps: list = field(default_factory=list) # last N timestamps
    consecutive_same: int = 0                       # stuck counter
    last_temp: Optional[float] = None
    last_time: float = 0.0
    total_readings: int = 0
    anomalies_detected: int = 0


# ══════════════════════════════════════════════════════════════════
#  Anomaly Detection Engine
# ══════════════════════════════════════════════════════════════════

class AnomalyDetector:
    """
    Multi-layer anomaly detector for cold-chain temperature sensors.

    Usage:
        detector = AnomalyDetector()
        result = detector.check("SHP_1", 4.5, "dairy")
        if result.is_anomaly:
            print(f"Anomaly! {result.reason}")
            # Use result.corrected_temp instead of original
    """

    def __init__(self):
        self._states = defaultdict(ShipmentSensorState)
        self._lock = threading.Lock()
        self._global_stats = {
            "total_readings": 0,
            "total_anomalies": 0,
            "l1_bounds": 0,
            "l2_rate": 0,
            "l3_zscore": 0,
            "l4_stuck": 0,
        }

    def check(self, shipment_id: str, temp: float, product_type: str = "default") -> AnomalyResult:
        """
        Run all 4 layers of anomaly detection on a temperature reading.
        Returns AnomalyResult with is_anomaly flag and reason.
        """
        with self._lock:
            state = self._states[shipment_id]
            state.total_readings += 1
            self._global_stats["total_readings"] += 1
            now = time.time()

            # ── L1: Physical Bounds Check ─────────────────────────
            result = self._check_bounds(temp, product_type)
            if result.is_anomaly:
                state.anomalies_detected += 1
                self._global_stats["total_anomalies"] += 1
                self._global_stats["l1_bounds"] += 1
                return result

            # ── L2: Rate-of-Change Check ──────────────────────────
            result = self._check_rate(temp, state, now)
            if result.is_anomaly:
                state.anomalies_detected += 1
                self._global_stats["total_anomalies"] += 1
                self._global_stats["l2_rate"] += 1
                # Don't update history with anomalous reading
                return result

            # ── L3: Statistical Z-Score Check ─────────────────────
            result = self._check_zscore(temp, state)
            if result.is_anomaly:
                state.anomalies_detected += 1
                self._global_stats["total_anomalies"] += 1
                self._global_stats["l3_zscore"] += 1
                return result

            # ── L4: Stuck Sensor Check ────────────────────────────
            result = self._check_stuck(temp, state)
            if result.is_anomaly:
                state.anomalies_detected += 1
                self._global_stats["total_anomalies"] += 1
                self._global_stats["l4_stuck"] += 1
                # This is a warning, we still accept the reading
                # but flag it for maintenance alert

            # ── Update rolling state ──────────────────────────────
            state.history.append(temp)
            state.timestamps.append(now)
            if len(state.history) > WINDOW_SIZE:
                state.history.pop(0)
                state.timestamps.pop(0)
            state.last_temp = temp
            state.last_time = now

            return AnomalyResult(is_anomaly=False, original_temp=temp)

    def _check_bounds(self, temp: float, product_type: str) -> AnomalyResult:
        """L1: Reject physically impossible values."""
        key = product_type.lower().replace(" ", "_")
        bounds = PHYSICAL_LIMITS.get(key, PHYSICAL_LIMITS["default"])

        if temp < bounds[0] or temp > bounds[1]:
            return AnomalyResult(
                is_anomaly=True,
                layer="L1_BOUNDS",
                reason=f"Temp {temp}°C outside physical limits [{bounds[0]}, {bounds[1]}] for {product_type}",
                original_temp=temp,
                corrected_temp=None,  # no safe fallback — discard entirely
            )
        return AnomalyResult(is_anomaly=False, original_temp=temp)

    def _check_rate(self, temp: float, state: ShipmentSensorState, now: float) -> AnomalyResult:
        """L2: Detect impossibly fast temperature changes (sensor fault)."""
        if state.last_temp is None:
            return AnomalyResult(is_anomaly=False, original_temp=temp)

        elapsed = max(now - state.last_time, 0.1)  # avoid div/0
        change = abs(temp - state.last_temp)

        # Scale max rate by time elapsed (longer gaps allow more change)
        allowed_change = MAX_RATE_OF_CHANGE * max(elapsed, 1)

        if change > allowed_change:
            return AnomalyResult(
                is_anomaly=True,
                layer="L2_RATE",
                reason=f"Temp jumped {change:.1f}°C in {elapsed:.0f}s (max allowed: {allowed_change:.1f}°C)",
                original_temp=temp,
                corrected_temp=state.last_temp,  # fall back to last good reading
            )
        return AnomalyResult(is_anomaly=False, original_temp=temp)

    def _check_zscore(self, temp: float, state: ShipmentSensorState) -> AnomalyResult:
        """L3: Flag readings that are statistical outliers from rolling mean."""
        if len(state.history) < 5:
            return AnomalyResult(is_anomaly=False, original_temp=temp)

        mean = sum(state.history) / len(state.history)
        variance = sum((t - mean) ** 2 for t in state.history) / len(state.history)
        std_dev = variance ** 0.5

        if std_dev < 0.01:  # avoid div/0 when readings are very stable
            return AnomalyResult(is_anomaly=False, original_temp=temp)

        z_score = abs(temp - mean) / std_dev

        if z_score > ZSCORE_THRESHOLD:
            return AnomalyResult(
                is_anomaly=True,
                layer="L3_ZSCORE",
                reason=f"Z-score {z_score:.1f} exceeds {ZSCORE_THRESHOLD} (temp={temp}°C, mean={mean:.1f}°C, σ={std_dev:.2f})",
                original_temp=temp,
                corrected_temp=mean,  # fall back to rolling mean
            )
        return AnomalyResult(is_anomaly=False, original_temp=temp)

    def _check_stuck(self, temp: float, state: ShipmentSensorState) -> AnomalyResult:
        """L4: Detect frozen/stuck sensors producing identical readings."""
        if state.last_temp is not None and abs(temp - state.last_temp) < STUCK_EPSILON:
            state.consecutive_same += 1
        else:
            state.consecutive_same = 0

        if state.consecutive_same >= STUCK_SENSOR_COUNT:
            return AnomalyResult(
                is_anomaly=True,
                layer="L4_STUCK",
                reason=f"Sensor stuck: {state.consecutive_same} identical readings ({temp}°C)",
                original_temp=temp,
                corrected_temp=temp,  # value might be correct, but sensor needs maintenance
            )
        return AnomalyResult(is_anomaly=False, original_temp=temp)

    def get_stats(self) -> dict:
        """Return global anomaly detection stats for dashboard."""
        with self._lock:
            return dict(self._global_stats)

    def get_shipment_stats(self, shipment_id: str) -> dict:
        """Return anomaly stats for a specific shipment."""
        with self._lock:
            state = self._states.get(shipment_id)
            if not state:
                return {"total_readings": 0, "anomalies_detected": 0}
            return {
                "total_readings": state.total_readings,
                "anomalies_detected": state.anomalies_detected,
                "anomaly_rate": round(state.anomalies_detected / max(state.total_readings, 1) * 100, 2),
                "history_size": len(state.history),
            }


# ── Global singleton ──────────────────────────────────────────────
detector = AnomalyDetector()
