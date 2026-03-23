"""
LiveCold Metrics Pipeline - Pure Pathway Framework
Real-time metrics collection using Pathway streaming transformations
"""

import pathway as pw
from datetime import datetime
import json


# Schema definitions
class AlertEventSchema(pw.Schema):
    """Schema for alert events"""
    timestamp: float
    event_timestamp: float  # Original sensor event time
    shipment_id: str
    alert_type: str
    risk_probability: float
    temp_c: float
    threshold_c: float
    cargo_type: str
    cargo_value_inr: float


class DecisionEventSchema(pw.Schema):
    """Schema for decision events"""
    timestamp: float
    shipment_id: str
    cargo_type: str
    decision: str  # "divert", "continue", "salvage"
    risk_before: float
    risk_after: float
    cost_inr: float
    co2_delta_kg: float
    waste_avoided_kg: float
    cargo_value_inr: float


class PathwayMetricsPipeline:
    """Real-time metrics using Pathway streaming"""
    
    def __init__(self):
        # CO2 emission factors (kg CO2e per kg food)
        self.emission_factors = {
            "dairy": 8.5,
            "meat": 27.0,
            "poultry": 6.9,
            "seafood": 5.1,
            "vegetables": 2.0,
            "fruits": 1.1,
            "grains": 2.5,
            "frozen": 3.2,
        }
    
    def build_metrics_pipeline(
        self,
        alerts_source: pw.Table,
        decisions_source: pw.Table
    ):
        """
        Build Pathway pipeline for real-time metrics
        
        Args:
            alerts_source: Pathway table with alert events
            decisions_source: Pathway table with decision events
        
        Returns:
            Dictionary with metric tables
        """
        
        print("Building Pathway metrics pipeline...")
        
        # ==== LATENCY METRICS ====
        # Calculate event-to-alert latency
        latencies = alerts_source.select(
            shipment_id=pw.this.shipment_id,
            alert_timestamp=pw.this.timestamp,
            event_timestamp=pw.this.event_timestamp,
            latency_ms=(pw.this.timestamp - pw.this.event_timestamp) * 1000,
            latency_category=pw.apply(
                self._categorize_latency,
                (pw.this.timestamp - pw.this.event_timestamp) * 1000
            )
        )
        
        # ==== ALERT ANALYSIS ====
        # Add computed fields to alerts
        alerts_enriched = alerts_source.select(
            *pw.this,
            temp_delta=pw.this.temp_c - pw.this.threshold_c,
            is_critical=pw.this.risk_probability > 0.7
        )
        
        # ==== DECISION METRICS ====
        # Calculate green impact metrics
        @pw.udf
        def calculate_co2_avoided(waste_kg: float, cargo_type: str) -> float:
            factor = self.emission_factors.get(cargo_type.lower(), 5.0)
            return waste_kg * factor
        
        @pw.udf
        def calculate_net_co2(co2_avoided: float, co2_delta: float) -> float:
            return co2_avoided - co2_delta
        
        @pw.udf
        def calculate_value_saved(
            risk_before: float,
            risk_after: float,
            cost: float,
            cargo_value: float
        ) -> float:
            expected_loss_without = risk_before * cargo_value
            expected_loss_with = risk_after * cargo_value + cost
            return expected_loss_without - expected_loss_with
        
        decisions_enriched = decisions_source.select(
            *pw.this,
            co2_avoided_kg=calculate_co2_avoided(
                pw.this.waste_avoided_kg,
                pw.this.cargo_type
            ),
            net_co2_impact_kg=calculate_net_co2(
                calculate_co2_avoided(
                    pw.this.waste_avoided_kg,
                    pw.this.cargo_type
                ),
                pw.this.co2_delta_kg
            ),
            value_saved_inr=calculate_value_saved(
                pw.this.risk_before,
                pw.this.risk_after,
                pw.this.cost_inr,
                pw.this.cargo_value_inr
            )
        )
        
        # ==== AGGREGATED METRICS ====
        # Per-shipment aggregations
        shipment_metrics = decisions_enriched.groupby(pw.this.shipment_id).reduce(
            shipment_id=pw.this.shipment_id,
            total_decisions=pw.reducers.count(),
            total_waste_avoided=pw.reducers.sum(pw.this.waste_avoided_kg),
            total_co2_avoided=pw.reducers.sum(pw.this.net_co2_impact_kg),
            total_value_saved=pw.reducers.sum(pw.this.value_saved_inr),
            avg_risk_before=pw.reducers.avg(pw.this.risk_before),
            avg_risk_after=pw.reducers.avg(pw.this.risk_after)
        )
        
        # Global aggregations (all shipments)
        # Note: latencies and decisions are separate tables (different universes),
        # so we aggregate them separately
        global_metrics = decisions_enriched.reduce(
            total_decisions=pw.reducers.count(),
            total_waste_avoided_kg=pw.reducers.sum(pw.this.waste_avoided_kg),
            total_co2_avoided_kg=pw.reducers.sum(pw.this.net_co2_impact_kg),
            total_value_saved_inr=pw.reducers.sum(pw.this.value_saved_inr),
        )

        # Latency aggregation (separate table)
        global_latency_metrics = latencies.reduce(
            avg_latency_ms=pw.reducers.avg(pw.this.latency_ms),
            max_latency_ms=pw.reducers.max(pw.this.latency_ms),
            min_latency_ms=pw.reducers.min(pw.this.latency_ms),
        )
        
        # Alert counts by type
        alert_counts = alerts_enriched.groupby(pw.this.alert_type).reduce(
            alert_type=pw.this.alert_type,
            count=pw.reducers.count(),
            avg_risk=pw.reducers.avg(pw.this.risk_probability),
            critical_count=pw.reducers.sum(
                pw.cast(int, pw.this.is_critical)
            )
        )
        
        # Decision breakdown
        decision_breakdown = decisions_enriched.groupby(pw.this.decision).reduce(
            decision_type=pw.this.decision,
            count=pw.reducers.count(),
            total_cost=pw.reducers.sum(pw.this.cost_inr),
            total_value_saved=pw.reducers.sum(pw.this.value_saved_inr)
        )
        
        print("✓ Metrics pipeline built")
        
        return {
            "latencies": latencies,
            "alerts": alerts_enriched,
            "decisions": decisions_enriched,
            "shipment_metrics": shipment_metrics,
            "global_metrics": global_metrics,
            "global_latency_metrics": global_latency_metrics,
            "alert_counts": alert_counts,
            "decision_breakdown": decision_breakdown
        }
    
    @staticmethod
    def _categorize_latency(latency_ms: float) -> str:
        """Categorize latency"""
        if latency_ms < 100:
            return "excellent"
        elif latency_ms < 500:
            return "good"
        elif latency_ms < 2000:
            return "acceptable"
        else:
            return "needs_improvement"
    
    def write_metrics_to_files(self, metrics_dict: dict, output_dir="./metrics"):
        """
        Write metrics tables to CSV files using Pathway output connectors
        
        Args:
            metrics_dict: Dictionary of metric tables from build_metrics_pipeline
            output_dir: Directory to write files
        """
        
        print(f"Writing metrics to {output_dir}/...")
        
        # Write each metric table to CSV
        pw.io.csv.write(
            metrics_dict["latencies"],
            f"{output_dir}/latencies.csv"
        )
        
        pw.io.csv.write(
            metrics_dict["alerts"],
            f"{output_dir}/alerts.csv"
        )
        
        pw.io.csv.write(
            metrics_dict["decisions"],
            f"{output_dir}/decisions.csv"
        )
        
        pw.io.csv.write(
            metrics_dict["shipment_metrics"],
            f"{output_dir}/shipment_metrics.csv"
        )
        
        pw.io.csv.write(
            metrics_dict["global_metrics"],
            f"{output_dir}/global_metrics.csv"
        )
        
        pw.io.csv.write(
            metrics_dict["global_latency_metrics"],
            f"{output_dir}/global_latency_metrics.csv"
        )
        
        pw.io.csv.write(
            metrics_dict["alert_counts"],
            f"{output_dir}/alert_counts.csv"
        )
        
        pw.io.csv.write(
            metrics_dict["decision_breakdown"],
            f"{output_dir}/decision_breakdown.csv"
        )
        
        print("✓ Metric outputs configured")


# Example: Create demo data sources
def create_demo_alert_stream():
    """Create a demo alert stream from CSV"""
    return pw.io.csv.read(
        "./demo_data/alerts.csv",
        schema=AlertEventSchema,
        mode="streaming"
    )


def create_demo_decision_stream():
    """Create a demo decision stream from CSV"""
    return pw.io.csv.read(
        "./demo_data/decisions.csv",
        schema=DecisionEventSchema,
        mode="streaming"
    )


if __name__ == "__main__":
    """Demo: Run metrics pipeline standalone"""
    
    import os
    os.makedirs("./demo_data", exist_ok=True)
    os.makedirs("./metrics", exist_ok=True)
    
    # Create demo data files
    with open("./demo_data/alerts.csv", "w") as f:
        f.write("timestamp,event_timestamp,shipment_id,alert_type,risk_probability,temp_c,threshold_c,cargo_type,cargo_value_inr\n")
        f.write("1000.0,999.5,S001,temp_high,0.65,12.0,6.0,dairy,250000\n")
        f.write("2000.0,1999.3,S002,door_open,0.25,9.0,8.0,vegetables,80000\n")
    
    with open("./demo_data/decisions.csv", "w") as f:
        f.write("timestamp,shipment_id,cargo_type,decision,risk_before,risk_after,cost_inr,co2_delta_kg,waste_avoided_kg,cargo_value_inr\n")
        f.write("1100.0,S001,dairy,divert,0.65,0.05,8500,15.0,150.0,250000\n")
        f.write("2100.0,S002,vegetables,continue,0.25,0.25,0,0,0,80000\n")
    
    # Build pipeline
    pipeline = PathwayMetricsPipeline()
    
    alerts = create_demo_alert_stream()
    decisions = create_demo_decision_stream()
    
    metrics = pipeline.build_metrics_pipeline(alerts, decisions)
    pipeline.write_metrics_to_files(metrics)
    
    print("\n" + "="*60)
    print("Pathway Metrics Pipeline Running")
    print("="*60)
    print("📊 Processing alerts and decisions...")
    print("📁 Output: ./metrics/")
    print("="*60 + "\n")
    
    # Run Pathway
    pw.run()
