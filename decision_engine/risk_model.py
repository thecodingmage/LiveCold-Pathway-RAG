import math


def sigmoid(x: float) -> float:
    return 1 / (1 + math.exp(-x))


def compute_risk(shipment_state: dict) -> dict:
    temp = shipment_state.get("temp", 0)
    safe_min = shipment_state.get("safe_min_temp", 2)
    safe_max = shipment_state.get("safe_max_temp", 8)
    exposure_minutes = shipment_state.get("exposure_minutes", 0)
    eta_minutes = shipment_state.get("eta_minutes_remaining", 300)

    if temp > safe_max:
        deviation = temp - safe_max
    elif temp < safe_min:
        deviation = safe_min - temp
    else:
        deviation = 0

    exposure_factor = exposure_minutes * 0.2
    base_score = deviation * 1.8 + exposure_factor

    if deviation == 0:
        risk_probability = 0.05
        risk_level = "LOW"
    else:
        eta_factor = min(eta_minutes / 300, 1.5)
        adjusted_score = base_score * eta_factor
        risk_probability = sigmoid(adjusted_score)

        if risk_probability < 0.3:
            risk_level = "LOW"
        elif risk_probability < 0.6:
            risk_level = "MEDIUM"
        else:
            risk_level = "HIGH"

    return {
        "risk_probability": round(risk_probability, 3),
        "risk_score_raw": round(base_score, 3),
        "risk_level": risk_level
    }