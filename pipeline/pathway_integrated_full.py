"""
LiveCold Complete Integrated Pipeline - Pure Pathway Framework
Combines: Sensors → Risk Scoring → RAG → Decisions → Metrics
"""

import pathway as pw
from pathway.xpacks.llm import embedders, parsers, splitters, llms
from pathway.xpacks.llm.document_store import DocumentStore
import os
from datetime import datetime


# ===== SCHEMAS =====
class TempEventSchema(pw.Schema):
    """Temperature sensor events"""
    timestamp: float
    shipment_id: str
    sensor_id: str
    temp_c: float


class ShipmentMetaSchema(pw.Schema):
    """Shipment metadata"""
    shipment_id: str
    cargo_type: str
    cargo_value_inr: float
    threshold_c: float
    destination: str


# ===== CONFIGURATION =====
WATCHED_DOCS_DIR = "./watched_docs"
GEMINI_MODEL = "gemini/gemini-1.5-flash"
EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
RISK_THRESHOLD = 0.5  # Alert when risk > 0.5


class LiveColdIntegratedPipeline:
    """Complete end-to-end Pathway pipeline"""
    
    def __init__(self):
        # Verify environment
        if not os.getenv("GEMINI_API_KEY"):
            raise ValueError("GEMINI_API_KEY not set")
        
        os.makedirs(WATCHED_DOCS_DIR, exist_ok=True)
        os.makedirs("./output", exist_ok=True)
        os.makedirs("./metrics", exist_ok=True)
        
        self.doc_store = None
        self.gemini_llm = None
    
    def build_rag_component(self):
        """Build RAG DocumentStore component"""
        
        print("Building RAG component...")
        
        # Read documents with streaming
        documents = pw.io.fs.read(
            path=WATCHED_DOCS_DIR,
            format="binary",
            mode="streaming",
            with_metadata=True
        )
        
        # Setup components
        parser = parsers.UnstructuredParser(mode="single")
        text_splitter = splitters.TokenCountSplitter(
            min_tokens=100,
            max_tokens=500,
            encoding_name="cl100k_base"
        )
        embedder = embedders.SentenceTransformerEmbedder(
            model=EMBEDDING_MODEL
        )
        
        from pathway.stdlib.indexing.nearest_neighbors import BruteForceKnnFactory
        retriever_factory = BruteForceKnnFactory(
            embedder=embedder,
            dimensions=384,
            reserved_space=1000,
            metric="cosine"
        )
        
        # Create DocumentStore
        self.doc_store = DocumentStore(
            docs=documents,
            retriever_factory=retriever_factory,
            parser=parser,
            splitter=text_splitter
        )
        
        # Create Gemini LLM
        self.gemini_llm = llms.LiteLLMChat(
            model=GEMINI_MODEL,
            api_key=os.getenv("GEMINI_API_KEY"),
            temperature=0.0,
            capacity=10,
            retry_strategy=pw.asynchronous.FixedDelayRetryStrategy(
                max_retries=3,
                delay_ms=1000
            )
        )
        
        print("✓ RAG component ready")
        
        return self.doc_store, self.gemini_llm
    
    def build_main_pipeline(self):
        """Build the complete integrated pipeline"""
        
        print("\n" + "="*60)
        print("Building LiveCold Integrated Pipeline")
        print("="*60)
        
        # ===== 1. INPUT STREAMS =====
        print("\n[1/7] Setting up input streams...")
        
        # Temperature events from MQTT (or CSV for testing)
        # For production: use pw.io.mqtt.read()
        # For testing: use pw.io.csv.read()
        temp_events = pw.io.csv.read(
            "./data/temp_events.csv",
            schema=TempEventSchema,
            mode="streaming"
        )
        
        # Shipment metadata (static)
        shipments = pw.io.csv.read(
            "./data/shipments.csv",
            schema=ShipmentMetaSchema,
            mode="static"
        )
        
        print("  ✓ Input streams configured")
        
        # ===== 2. WINDOWING & FEATURE EXTRACTION =====
        print("[2/7] Computing rolling windows...")
        
        # Group by shipment and compute rolling window stats
        temp_windowed = temp_events.windowby(
            pw.this.shipment_id,
            window=pw.temporal.sliding(
                hop=pw.Duration.seconds(30),
                duration=pw.Duration.minutes(10)
            ),
            time_expr=pw.this.timestamp
        ).reduce(
            shipment_id=pw.this._pw_window_key,
            window_end=pw.this._pw_window_end,
            mean_temp=pw.reducers.avg(pw.this.temp_c),
            max_temp=pw.reducers.max(pw.this.temp_c),
            min_temp=pw.reducers.min(pw.this.temp_c),
            event_count=pw.reducers.count(),
            first_event_time=pw.reducers.min(pw.this.timestamp)
        )
        
        print("  ✓ Rolling windows computed")
        
        # ===== 3. JOIN WITH METADATA =====
        print("[3/7] Enriching with metadata...")
        
        enriched = temp_windowed.join(
            shipments,
            pw.left.shipment_id == pw.right.shipment_id
        ).select(
            shipment_id=pw.left.shipment_id,
            window_end=pw.left.window_end,
            mean_temp=pw.left.mean_temp,
            max_temp=pw.left.max_temp,
            threshold_c=pw.right.threshold_c,
            cargo_type=pw.right.cargo_type,
            cargo_value_inr=pw.right.cargo_value_inr,
            destination=pw.right.destination,
            event_count=pw.left.event_count,
            first_event_time=pw.left.first_event_time
        )
        
        print("  ✓ Data enriched")
        
        # ===== 4. RISK SCORING =====
        print("[4/7] Computing risk scores...")
        
        @pw.udf
        def compute_risk(max_temp: float, threshold: float) -> float:
            """Compute spoilage risk probability"""
            temp_delta = max_temp - threshold
            if temp_delta <= 0:
                return 0.0
            # Exponential risk curve
            k = 0.05
            risk = 1.0 - (2.71828 ** (-k * temp_delta ** 2))
            return min(risk, 0.99)
        
        risk_scores = enriched.select(
            *pw.this,
            temp_delta=pw.this.max_temp - pw.this.threshold_c,
            risk_probability=compute_risk(pw.this.max_temp, pw.this.threshold_c)
        )
        
        print("  ✓ Risk scores computed")
        
        # ===== 5. TRIGGER ALERTS =====
        print("[5/7] Detecting alerts...")
        
        alerts = risk_scores.filter(
            pw.this.risk_probability > RISK_THRESHOLD
        ).select(
            shipment_id=pw.this.shipment_id,
            timestamp=pw.this.window_end,
            event_timestamp=pw.this.first_event_time,
            alert_type="temp_high",
            risk_probability=pw.this.risk_probability,
            temp_c=pw.this.max_temp,
            threshold_c=pw.this.threshold_c,
            cargo_type=pw.this.cargo_type,
            cargo_value_inr=pw.this.cargo_value_inr,
            destination=pw.this.destination
        )
        
        print("  ✓ Alert detection configured")
        
        # ===== 6. DECISION OPTIMIZATION =====
        print("[6/7] Building decision optimizer...")
        
        @pw.udf
        def decide_action(
            risk: float,
            cargo_value: float,
            cargo_type: str
        ) -> dict:
            """Decide whether to divert or continue"""
            
            # Simple decision logic
            divert_cost = 10000  # Base cost in INR
            risk_after_divert = 0.05
            
            expected_loss_continue = risk * cargo_value
            expected_loss_divert = risk_after_divert * cargo_value + divert_cost
            
            if expected_loss_divert < expected_loss_continue:
                # Estimate cargo weight (simplified)
                cargo_weights = {
                    "dairy": 200, "meat": 300, "seafood": 250,
                    "vegetables": 150, "fruits": 120
                }
                waste_avoided = risk * cargo_weights.get(cargo_type.lower(), 200)
                
                return {
                    "decision": "divert",
                    "risk_before": risk,
                    "risk_after": risk_after_divert,
                    "cost_inr": divert_cost,
                    "co2_delta_kg": 15.0,
                    "waste_avoided_kg": waste_avoided
                }
            else:
                return {
                    "decision": "continue",
                    "risk_before": risk,
                    "risk_after": risk,
                    "cost_inr": 0.0,
                    "co2_delta_kg": 0.0,
                    "waste_avoided_kg": 0.0
                }
        
        decisions = alerts.select(
            *pw.this,
            decision_data=decide_action(
                pw.this.risk_probability,
                pw.this.cargo_value_inr,
                pw.this.cargo_type
            )
        )
        
        # Extract decision fields
        decisions_flat = decisions.select(
            timestamp=pw.this.timestamp,
            shipment_id=pw.this.shipment_id,
            cargo_type=pw.this.cargo_type,
            cargo_value_inr=pw.this.cargo_value_inr,
            decision=pw.this.decision_data["decision"],
            risk_before=pw.this.decision_data["risk_before"],
            risk_after=pw.this.decision_data["risk_after"],
            cost_inr=pw.this.decision_data["cost_inr"],
            co2_delta_kg=pw.this.decision_data["co2_delta_kg"],
            waste_avoided_kg=pw.this.decision_data["waste_avoided_kg"]
        )
        
        print("  ✓ Decision optimizer configured")
        
        # ===== 7. RAG RECOMMENDATIONS =====
        print("[7/7] Integrating RAG...")
        
        # Build RAG component
        doc_store, gemini_llm = self.build_rag_component()
        
        # Build query for RAG
        @pw.udf
        def build_rag_query(
            shipment_id: str,
            risk: float,
            temp: float,
            cargo_type: str
        ) -> str:
            return f"""A {cargo_type} shipment (ID: {shipment_id}) has temperature {temp}°C with spoilage risk {risk:.1%}.

According to SOPs, what immediate actions should be taken? Provide numbered checklist."""
        
        # Create queries table for DocumentStore
        rag_queries = alerts.select(
            query=build_rag_query(
                pw.this.shipment_id,
                pw.this.risk_probability,
                pw.this.temp_c,
                pw.this.cargo_type
            ),
            k=3,
            metadata_filter=None,
            filepath_globpattern=None,
            shipment_id=pw.this.shipment_id  # Keep for joining
        )
        
        # Retrieve documents
        retrieved_docs = doc_store.retrieve_query(rag_queries)
        
        # Extract contexts and build LLM prompts
        @pw.udf
        def extract_context(docs):
            """Extract text from retrieved documents"""
            texts = []
            for doc in docs:
                texts.append(doc.get("text", ""))
            return "\n\n".join(texts)
        
        @pw.udf
        def build_llm_prompt(query: str, context: str) -> str:
            return f"""Given context from SOPs:

{context}

Question: {query}

Instructions:
1. Provide numbered action checklist
2. Cite SOP sections: "Per SOP §X.Y, [action]"
3. End with: "Final decision requires human approval"

Answer:"""
        
        rag_contexts = retrieved_docs.select(
            shipment_id=rag_queries.shipment_id,
            query=rag_queries.query,
            context=extract_context(pw.this.result),
            prompt=build_llm_prompt(
                rag_queries.query,
                extract_context(pw.this.result)
            )
        )
        
        # Generate LLM answers
        rag_answers = rag_contexts.select(
            shipment_id=pw.this.shipment_id,
            rag_answer=gemini_llm(
                llms.prompt_chat_single_qa(pw.this.prompt)
            )
        )
        
        # Join decisions with RAG answers
        final_output = decisions_flat.join(
            rag_answers,
            pw.left.shipment_id == pw.right.shipment_id,
            how=pw.JoinMode.LEFT  # Left join in case RAG fails
        ).select(
            *pw.left,
            rag_recommendation=pw.right.rag_answer
        )
        
        print("  ✓ RAG integrated")
        
        # ===== 8. OUTPUTS =====
        print("\n[Output] Configuring outputs...")
        
        # Write alerts to CSV
        pw.io.csv.write(alerts, "./output/alerts.csv")
        
        # Write decisions to CSV
        pw.io.csv.write(decisions_flat, "./output/decisions.csv")
        
        # Write final output with RAG to CSV
        pw.io.csv.write(final_output, "./output/final_decisions_with_rag.csv")
        
        # Optionally write to Kafka (commented for now)
        # pw.io.kafka.write(
        #     final_output,
        #     topic="livecold-alerts",
        #     broker="localhost:9092",
        #     format="json"
        # )
        
        print("  ✓ Outputs configured")
        
        # ===== 9. METRICS =====
        print("[Metrics] Building metrics...")
        
        # Use metrics pipeline
        from pipeline.pathway_metrics_pipeline import PathwayMetricsPipeline
        
        metrics_pipeline = PathwayMetricsPipeline()
        metrics = metrics_pipeline.build_metrics_pipeline(
            alerts_source=alerts,
            decisions_source=decisions_flat
        )
        metrics_pipeline.write_metrics_to_files(metrics, "./metrics")
        
        print("  ✓ Metrics configured")
        
        print("\n" + "="*60)
        print("Pipeline Build Complete!")
        print("="*60)
        
        return final_output
    
    def run(self):
        """Run the complete pipeline"""
        
        # Build pipeline
        self.build_main_pipeline()
        
        print("\n" + "="*60)
        print("LiveCold Integrated Pipeline Starting")
        print("="*60)
        print("📡 Inputs: ./data/temp_events.csv, ./data/shipments.csv")
        print("📁 Documents: ./watched_docs/")
        print("📊 Outputs: ./output/")
        print("📈 Metrics: ./metrics/")
        print("🤖 LLM: " + GEMINI_MODEL)
        print("="*60 + "\n")
        
        # Start Pathway computation
        pw.run()


if __name__ == "__main__":
    """Run the integrated pipeline"""
    
    pipeline = LiveColdIntegratedPipeline()
    pipeline.run()
