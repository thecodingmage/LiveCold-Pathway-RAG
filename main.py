"""
LiveCold - Cold Chain Monitoring & Decision System
Unified entry point for all pipeline components.

Usage:
    python main.py rag          # Start RAG pipeline (REST API on port 8765)
    python main.py metrics      # Run metrics pipeline standalone demo
    python main.py full         # Run full integrated Pathway pipeline
    python main.py mqtt         # Start MQTT pipeline listener (requires MQTT broker)
    python main.py sim-temp     # Start temperature simulator (requires MQTT broker)
    python main.py sim-gps      # Start GPS simulator (requires MQTT broker)
    python main.py sim-reefer   # Start reefer telemetry simulator
    python main.py sim-door     # Start door/shock event simulator
    python main.py sim-all      # Start ALL simulators at once
    python main.py dashboard    # Start web dashboard (Flask)
"""

import sys


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        print("Available commands: rag, rag-v2, metrics, full, mqtt, sim-temp, sim-gps, sim-reefer, sim-door, sim-all, dashboard")
        return

    command = sys.argv[1].lower()

    if command == "rag":
        from pipeline.pathway_rag_pipeline import run_rag_pipeline
        run_rag_pipeline()

    elif command == "rag-v2":
        from pipeline.pathway_rag_pipeline_v2 import run_rag_pipeline
        run_rag_pipeline()

    elif command == "metrics":
        from pipeline.pathway_metrics_pipeline import PathwayMetricsPipeline, create_demo_alert_stream, create_demo_decision_stream
        import pathway as pw
        import os
        os.makedirs("./demo_data", exist_ok=True)
        os.makedirs("./metrics", exist_ok=True)

        # Create demo data if not exists
        if not os.path.exists("./demo_data/alerts.csv"):
            with open("./demo_data/alerts.csv", "w") as f:
                f.write("timestamp,event_timestamp,shipment_id,alert_type,risk_probability,temp_c,threshold_c,cargo_type,cargo_value_inr\n")
                f.write("1000.0,999.5,S001,temp_high,0.65,12.0,6.0,dairy,250000\n")

        if not os.path.exists("./demo_data/decisions.csv"):
            with open("./demo_data/decisions.csv", "w") as f:
                f.write("timestamp,shipment_id,cargo_type,decision,risk_before,risk_after,cost_inr,co2_delta_kg,waste_avoided_kg,cargo_value_inr\n")
                f.write("1100.0,S001,dairy,divert,0.65,0.05,8500,15.0,150.0,250000\n")

        pipeline = PathwayMetricsPipeline()
        alerts = create_demo_alert_stream()
        decisions = create_demo_decision_stream()
        metrics = pipeline.build_metrics_pipeline(alerts, decisions)
        pipeline.write_metrics_to_files(metrics)
        print("\nPathway Metrics Pipeline Running...")
        pw.run()

    elif command == "full":
        from pipeline.pathway_integrated_full import LiveColdIntegratedPipeline
        pipeline = LiveColdIntegratedPipeline()
        pipeline.run()

    elif command == "mqtt":
        from pipeline.livecold_pipeline import start_pipeline
        start_pipeline()

    elif command == "sim-temp":
        from sim.temp_simulator import main as sim_main
        sim_main()

    elif command == "sim-gps":
        from sim.gps_simulator import main as sim_main
        sim_main()

    elif command == "sim-reefer":
        from sim.reefer_simulator import main as sim_main
        sim_main()

    elif command == "sim-door":
        from sim.door_simulator import main as sim_main
        sim_main()

    elif command == "sim-all":
        import threading
        from sim.temp_simulator import main as temp_main
        from sim.gps_simulator import main as gps_main
        from sim.reefer_simulator import main as reefer_main
        from sim.door_simulator import main as door_main

        print("🚀 Starting ALL simulators...")
        threads = [
            threading.Thread(target=temp_main, daemon=True, name="TempSim"),
            threading.Thread(target=gps_main, daemon=True, name="GPSSim"),
            threading.Thread(target=reefer_main, daemon=True, name="ReeferSim"),
            threading.Thread(target=door_main, daemon=True, name="DoorSim"),
        ]
        for t in threads:
            t.start()
            print(f"  ✓ {t.name} started")

        try:
            while True:
                import time
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n🛑 All simulators stopped.")

    elif command == "dashboard":
        from dashboard.app import main as dash_main
        dash_main()

    elif command == "pathway-bridge":
        from pipeline.pathway_mqtt_bridge import build_pathway_pipeline
        build_pathway_pipeline()

    else:
        print(f"Unknown command: {command}")
        print(__doc__)


if __name__ == "__main__":
    main()
