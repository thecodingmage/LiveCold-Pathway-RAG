#!/bin/bash
# LiveCold Docker Entrypoint
# Starts Pathway RAG + dashboard + MQTT pipeline + simulators

set -e

echo "╔══════════════════════════════════════════════════╗"
echo "║   ❄️  LiveCold - Cold Chain Monitor (Docker)      ║"
echo "╚══════════════════════════════════════════════════╝"

# Wait for MQTT broker to be ready
echo "⏳ Waiting for MQTT broker..."
for i in $(seq 1 30); do
    if python3 -c "import socket; s=socket.socket(); s.settimeout(1); s.connect(('${MQTT_HOST:-mosquitto}', 1883)); s.close()" 2>/dev/null; then
        echo "✅ MQTT broker ready"
        break
    fi
    sleep 1
done

# Start Pathway RAG Pipeline (background)
echo "📚 Starting Pathway RAG Pipeline (port 8765)..."
python3 main.py rag &
RAG_PID=$!
sleep 3

# Start Dashboard (background)
echo "🌐 Starting Dashboard (port 5050)..."
python3 main.py dashboard &
DASH_PID=$!
sleep 2

# Start MQTT Pipeline (background)
echo "📡 Starting Pipeline..."
python3 main.py mqtt &
MQTT_PID=$!
sleep 1

# Start ALL simulators (background)
echo "🚀 Starting Simulators..."
python3 main.py sim-all &
SIM_PID=$!

echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║   ✅ LiveCold Running in Docker!                 ║"
echo "║                                                  ║"
echo "║   📊 Dashboard:   http://localhost:5050           ║"
echo "║   📚 Pathway RAG: http://localhost:8765           ║"
echo "║   📡 MQTT Pipeline: connected to broker          ║"
echo "║   🚚 Simulators: 25 shipments × 4 streams       ║"
echo "║                                                  ║"
echo "╚══════════════════════════════════════════════════╝"

# Wait and cleanup on exit
cleanup() {
    echo ""
    echo "🛑 Stopping all components..."
    kill $RAG_PID $DASH_PID $MQTT_PID $SIM_PID 2>/dev/null
    echo "✅ Stopped."
}
trap cleanup EXIT INT TERM

wait
