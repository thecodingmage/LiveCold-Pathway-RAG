#!/bin/bash
# LiveCold Demo Replay Script
# Starts all components for a quick demo presentation

set -e

echo "╔══════════════════════════════════════════════════╗"
echo "║   ❄️  LiveCold - Cold Chain Demo Replay          ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""

# Check Mosquitto
if ! lsof -i :1883 > /dev/null 2>&1; then
    # Check if Mosquitto is already running
    if ! pgrep -x "mosquitto" > /dev/null; then
        echo "⚠️  Starting Mosquitto..."
        /opt/homebrew/sbin/mosquitto -d 2>/dev/null || brew services start mosquitto
        sleep 2
    fi
fi
echo "✅ MQTT Broker ready (port 1883)"

# Activate slim venv (pathway + sentence-transformers, no pathway[all])
source .venv-slim/bin/activate

# Start Pathway RAG Pipeline (background) — DocumentStore + LLM xPack
echo "📚 Starting Pathway RAG Pipeline (port 8765)..."
python3 main.py rag &
RAG_PID=$!
sleep 3

# Start Pathway MQTT Bridge (background) — sensor events → Pathway tables
echo "📊 Starting Pathway MQTT Bridge..."
python3 main.py pathway-bridge &
BRIDGE_PID=$!
sleep 2

# Start Dashboard (background) — calls Pathway RAG for SOP recommendations
echo "🌐 Starting Dashboard (port 5050)..."
python3 main.py dashboard &
DASH_PID=$!
sleep 2

# Start MQTT Pipeline (background)
echo "📡 Starting MQTT Pipeline..."
python3 main.py mqtt &
MQTT_PID=$!
sleep 1

# Start ALL simulators (background)
echo "🚀 Starting Simulators..."
python3 main.py sim-all &
SIM_PID=$!

echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║   ✅ LiveCold Demo Running!                      ║"
echo "║                                                  ║"
echo "║   📊 Dashboard:     http://localhost:5050         ║"
echo "║   📚 Pathway RAG:   http://localhost:8765         ║"
echo "║   📊 Pathway MQTT:  processing sensor events     ║"
echo "║   📡 MQTT Pipeline: listening on 1883            ║"
echo "║   🚚 Simulators: 25 shipments × 4 streams       ║"
echo "║                                                  ║"
echo "║   Press Ctrl+C to stop all components            ║"
echo "╚══════════════════════════════════════════════════╝"

echo ""

# Wait and cleanup on exit
cleanup() {
    echo ""
    echo "🛑 Stopping all components..."
    kill $RAG_PID $BRIDGE_PID $DASH_PID $MQTT_PID $SIM_PID 2>/dev/null
    echo "✅ Demo stopped."
}
trap cleanup EXIT INT TERM

wait
