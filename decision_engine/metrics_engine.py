class MetricsEngine:
    def __init__(self):
        self.total_events           = 0
        self.total_value_monitored  = 0
        self.total_expected_loss    = 0
        self.total_diversions       = 0
        self.total_high_risk_events = 0
        self.total_co2_delta        = 0
        self.total_value_saved      = 0   # ₹ prevented from being lost via diversions

    def update(self, shipment_state: dict, decision_output: dict):
        self.total_events += 1

        value = shipment_state.get("value_inr", 0)
        risk  = shipment_state.get("risk_probability", 0)

        self.total_value_monitored += value
        self.total_expected_loss   += decision_output.get("expected_loss_continue", 0)

        if risk > 0.5:
            self.total_high_risk_events += 1

        if decision_output.get("recommended_action") == "DIVERT":
            self.total_diversions += 1

            # Value saved = what we would have lost if we continued minus diversion cost
            loss_if_continued = decision_output.get("expected_loss_continue", 0)
            diversion_cost    = decision_output.get("diversion_cost_inr", 0)
            net_saving        = loss_if_continued - diversion_cost
            if net_saving > 0:
                self.total_value_saved += net_saving

        self.total_co2_delta += decision_output.get("co2_delta_kg", 0)

    def summary(self):
        diversion_rate = 0
        if self.total_events > 0:
            diversion_rate = (self.total_diversions / self.total_events) * 100

        return {
            "total_events_processed":    self.total_events,
            "total_value_monitored_inr": round(self.total_value_monitored, 2),
            "total_expected_loss_inr":   round(self.total_expected_loss, 2),
            "total_high_risk_events":    self.total_high_risk_events,
            "total_diversions":          self.total_diversions,
            "diversion_rate_percent":    round(diversion_rate, 2),
            "total_co2_delta_kg":        round(self.total_co2_delta, 2),
            # ── Demo-friendly stat: money our system saved ──
            "cargo_value_saved_inr":     round(self.total_value_saved, 2),
        }