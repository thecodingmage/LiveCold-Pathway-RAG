# ── Constants (easy to tune without diving into logic) ─────────────────────────
FUEL_COST_PER_KM = 40          # ₹ per km of diversion
EMISSION_FACTOR = 0.25         # kg CO₂ per km
RISK_REDUCTION_IF_DIVERT = 0.7 # 70% risk reduction when shipment is diverted
DIVERT_RISK_THRESHOLD = 0.6    # minimum risk_probability to even consider diversion
ECO_CO2_PENALTY_THRESHOLD = 20 # kg CO₂ — above this, eco mode avoids diversion
# ───────────────────────────────────────────────────────────────────────────────


def compute_diversion_decision(shipment_state: dict) -> dict:
    risk_p         = shipment_state.get("risk_probability", 0)
    value          = shipment_state.get("value_inr", 0)
    extra_km       = shipment_state.get("nearest_hub_distance_km", 0)
    objective_mode = shipment_state.get("objective_mode", "cost")

    # ── Core cost calculations ──────────────────────────────────────────────────
    expected_loss_continue = risk_p * value

    diversion_cost       = extra_km * FUEL_COST_PER_KM
    reduced_risk         = risk_p * (1 - RISK_REDUCTION_IF_DIVERT)
    expected_loss_divert = reduced_risk * value
    total_divert_cost    = diversion_cost + expected_loss_divert

    co2_delta = extra_km * EMISSION_FACTOR
    # ───────────────────────────────────────────────────────────────────────────

    # ── Decision logic (supports cost / eco modes) ─────────────────────────────

    # Gate 1: Risk must cross the threshold to even consider diversion
    if risk_p < DIVERT_RISK_THRESHOLD:
        action = "CONTINUE"
        reason = "Risk below threshold"

    # Gate 2: Risk is high enough — now evaluate by objective mode
    else:
        if objective_mode == "eco":
            # In eco mode, avoid diversion if CO₂ penalty is too high,
            # unless the cargo loss would be catastrophic (> 10× diversion cost)
            if co2_delta > ECO_CO2_PENALTY_THRESHOLD and total_divert_cost < expected_loss_continue * 0.5:
                action = "CONTINUE"
                reason = "Diversion avoided to minimise CO₂ emissions"
            elif total_divert_cost < expected_loss_continue:
                action = "DIVERT"
                reason = "Eco-mode: cost optimal and CO₂ within limits"
            else:
                action = "CONTINUE"
                reason = "Diversion not economical (eco mode)"

        else:  # default: "cost" mode
            if total_divert_cost < expected_loss_continue:
                action = "DIVERT"
                reason = "Cost optimal under high risk"
            else:
                action = "CONTINUE"
                reason = "Diversion not economical"
    # ───────────────────────────────────────────────────────────────────────────

    return {
        "recommended_action":    action,
        "expected_loss_continue": round(expected_loss_continue, 2),
        "diversion_cost_inr":    round(diversion_cost, 2),
        "co2_delta_kg":          round(co2_delta, 2),
        "decision_reason":       reason,
        "objective_mode":        objective_mode,
    }