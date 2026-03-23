from decision_engine.risk_model import compute_risk
from decision_engine.diversion_optimizer import compute_diversion_decision
from decision_engine.metrics_engine import MetricsEngine
from datetime import datetime

# Global metrics instance
metrics_engine = MetricsEngine()


def evaluate_shipment(shipment_state: dict) -> dict:
    """
    Main intelligence entry point.
    Takes shipment_state and returns final evaluated output.
    """

    # ---- Step 1: Compute Risk ----
    risk_output = compute_risk(shipment_state)
    shipment_state["risk_probability"] = risk_output["risk_probability"]

    # ---- Step 2: Compute Diversion Decision ----
    decision_output = compute_diversion_decision(shipment_state)

    # ---- Step 3: Update Metrics ----
    metrics_engine.update(shipment_state, decision_output)

    # ---- Step 4: Build Final Structured Output ----
    final_output = {
        "shipment_id": shipment_state.get("shipment_id"),
        "lat": shipment_state.get("lat"),
        "lon": shipment_state.get("lon"),
        "temp": shipment_state.get("temp"),
        "risk_probability": shipment_state.get("risk_probability"),
        "recommended_action": decision_output["recommended_action"],
        "expected_loss_inr": decision_output["expected_loss_continue"],
        "diversion_cost_inr": decision_output["diversion_cost_inr"],
        "co2_delta_kg": decision_output["co2_delta_kg"],
        "decision_reason": decision_output["decision_reason"],
        "timestamp": datetime.now().isoformat()
    }

    return final_output


def get_metrics_summary():
    """
    Expose system metrics for dashboard.
    """
    return metrics_engine.summary()