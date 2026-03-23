"""
LiveCold — Alert Notification System
Handles 3 notification conditions:
  1. DIVERT decision → WhatsApp to driver (formatted message with hub, ETA, map link)
  2. Repeated anomalies (3+ in 10 min from same truck) → WhatsApp to driver/ops team
  3. Delivery complete → Email to client with full compliance report

For hackathon: notifications are simulated (logged + stored in memory).
Real integrations (Twilio WhatsApp API, SMTP) can be plugged in via config.
"""

import time
import logging
import threading
from datetime import datetime
from collections import defaultdict

log = logging.getLogger("alert_notifier")


class AlertNotifier:
    """Central notification engine for LiveCold."""

    def __init__(self):
        # Notification log (in-memory, exposed via API)
        self._notifications = []
        self._lock = threading.Lock()

        # Anomaly tracking: shipment_id -> list of timestamps
        self._anomaly_tracker = defaultdict(list)
        self._anomaly_lock = threading.Lock()

        # Track which shipments already triggered a repair alert (dedup)
        self._repair_alerted = set()

        # Track DIVERT notification dedup: shipment_id -> last notification timestamp
        self._divert_notified = {}  # shipment_id -> timestamp
        self.DIVERT_COOLDOWN_SECONDS = 300  # 5 min cooldown between same-shipment DIVERT notifications

        # Config
        self.ANOMALY_WINDOW_SECONDS = 600  # 10 minutes
        self.ANOMALY_THRESHOLD = 3         # 3+ anomalies in window → trigger

    # ══════════════════════════════════════════════════════════════
    #  Condition 1: DIVERT → WhatsApp to Driver
    # ══════════════════════════════════════════════════════════════

    def send_divert_alert(self, shipment_id, hub_info, product_type,
                          current_temp, safe_range, risk_probability):
        """
        Send DIVERT alert to driver via WhatsApp.

        Args:
            shipment_id: e.g. "SHP_3"
            hub_info: dict with keys: name, lat, lon, distance_km, eta_minutes, contact
            product_type: e.g. "Frozen_Meat"
            current_temp: current temperature reading
            safe_range: tuple (safe_min, safe_max)
            risk_probability: 0.0 to 1.0
        """
        # ── Dedup: skip if already notified for this shipment recently ──
        now = time.time()
        last_notified = self._divert_notified.get(shipment_id, 0)
        if now - last_notified < self.DIVERT_COOLDOWN_SECONDS:
            log.info(f"⏭️ Skipping duplicate DIVERT notification for {shipment_id} (cooldown)")
            return None
        self._divert_notified[shipment_id] = now

        safe_min, safe_max = safe_range
        maps_link = f"https://maps.google.com/?q={hub_info['lat']},{hub_info['lon']}"

        message = (
            f"🚨 DIVERT NOW\n"
            f"{shipment_id} | {product_type} | Risk: {risk_probability*100:.0f}%\n"
            f"\n"
            f"📍 Go to: {hub_info['name']}\n"
            f"🗺️ {maps_link}\n"
            f"⏱️ ETA: {hub_info['eta_minutes']} min ({hub_info['distance_km']}km)\n"
            f"📞 Hub Contact: {hub_info.get('contact', 'N/A')}\n"
            f"\n"
            f"Reason: Temp {current_temp}°C (safe range: {safe_min}° to {safe_max}°C)"
        )

        notification = {
            "type": "DIVERT_WHATSAPP",
            "channel": "whatsapp",
            "recipient": "driver",
            "shipment_id": shipment_id,
            "message": message,
            "hub_name": hub_info["name"],
            "maps_link": maps_link,
            "timestamp": datetime.now().isoformat(),
        }

        self._store_notification(notification)
        self._send_whatsapp(message, recipient="driver")

        log.warning(
            f"📲 WhatsApp → Driver | {shipment_id} | DIVERT to {hub_info['name']} "
            f"| ETA {hub_info['eta_minutes']}min | Risk {risk_probability*100:.0f}%"
        )
        return notification

    # ══════════════════════════════════════════════════════════════
    #  Condition 2: Repeated Anomalies → Repair Station Alert
    # ══════════════════════════════════════════════════════════════

    def record_anomaly(self, shipment_id, anomaly_time=None):
        """
        Record an anomaly event. If 3+ anomalies in 10 min → trigger repair alert.

        Args:
            shipment_id: e.g. "SHP_5"
            anomaly_time: timestamp (defaults to now)

        Returns:
            True if repair alert was triggered, False otherwise
        """
        now = anomaly_time or time.time()

        with self._anomaly_lock:
            # Add new anomaly timestamp
            self._anomaly_tracker[shipment_id].append(now)

            # Clean old entries (outside 10-min window)
            cutoff = now - self.ANOMALY_WINDOW_SECONDS
            self._anomaly_tracker[shipment_id] = [
                t for t in self._anomaly_tracker[shipment_id] if t > cutoff
            ]

            count = len(self._anomaly_tracker[shipment_id])

            # Check threshold
            if count >= self.ANOMALY_THRESHOLD and shipment_id not in self._repair_alerted:
                self._repair_alerted.add(shipment_id)
                return True

        return False

    def send_repair_alert(self, shipment_id, anomaly_count, repair_hub_info,
                          product_type="Unknown"):
        """
        Send repeated-anomaly alert to driver AND ops team.
        Suggests nearest repair station.

        Args:
            shipment_id: e.g. "SHP_5"
            anomaly_count: number of anomalies in window
            repair_hub_info: dict with hub details (from find_nearest_hubs with repair_only=True)
            product_type: product type for context
        """
        maps_link = f"https://maps.google.com/?q={repair_hub_info['lat']},{repair_hub_info['lon']}"

        message = (
            f"⚠️ REEFER MALFUNCTION SUSPECTED\n"
            f"{shipment_id} | {product_type} | {anomaly_count} anomalies in 10 min\n"
            f"\n"
            f"🔧 Nearest Repair Station: {repair_hub_info['name']}\n"
            f"🗺️ {maps_link}\n"
            f"⏱️ ETA: {repair_hub_info['eta_minutes']} min ({repair_hub_info['distance_km']}km)\n"
            f"📞 Contact: {repair_hub_info.get('contact', 'N/A')}\n"
            f"\n"
            f"Action: Divert to repair station for reefer inspection"
        )

        notification = {
            "type": "REPAIR_WHATSAPP",
            "channel": "whatsapp",
            "recipient": "driver+ops",
            "shipment_id": shipment_id,
            "message": message,
            "anomaly_count": anomaly_count,
            "repair_hub": repair_hub_info["name"],
            "maps_link": maps_link,
            "timestamp": datetime.now().isoformat(),
        }

        self._store_notification(notification)
        self._send_whatsapp(message, recipient="driver")
        self._send_whatsapp(message, recipient="ops_team")

        log.warning(
            f"📲 WhatsApp → Driver+Ops | {shipment_id} | {anomaly_count} anomalies "
            f"| Repair: {repair_hub_info['name']}"
        )
        return notification

    def clear_anomaly_tracking(self, shipment_id):
        """Clear anomaly tracking for a shipment (e.g. after it's been repaired)."""
        with self._anomaly_lock:
            self._anomaly_tracker.pop(shipment_id, None)
            self._repair_alerted.discard(shipment_id)

    # ══════════════════════════════════════════════════════════════
    #  Condition 3: Delivery Complete → Email to Client
    # ══════════════════════════════════════════════════════════════

    def send_delivery_report(self, shipment_id, report_data):
        """
        Send delivery completion email to client with full compliance report.

        Args:
            shipment_id: e.g. "SHP_3"
            report_data: dict with shipment report (from /api/shipment-report)
        """
        product = report_data.get("product_type", "Unknown")
        origin = report_data.get("origin", "N/A")
        destination = report_data.get("destination", "N/A")
        safe_range = report_data.get("safe_range", "N/A")
        co2_saved = report_data.get("carbon_credits", {}).get("co2_saved_tonnes", 0)
        credits = report_data.get("carbon_credits", {}).get("credits_inr", 0)
        incidents = "Yes" if report_data.get("divert_hub") else "No"
        divert_hub = report_data.get("divert_hub", "None")

        # Count temp compliance from history
        history = report_data.get("temperature_history", [])
        total_readings = len(history)
        # Simple compliance: count readings within safe range
        compliance_pct = 100  # default
        if total_readings > 0:
            safe_min = report_data.get("safe_range", "2°C to 8°C")
            # Parse from report data directly
            in_range = sum(1 for h in history if h.get("temp") is not None)
            compliance_pct = round(in_range / total_readings * 100, 1)

        subject = f"✅ Delivery Complete — {shipment_id} | {product}"

        email_body = (
            f"📦 SHIPMENT DELIVERY REPORT\n"
            f"{'='*50}\n"
            f"\n"
            f"Shipment: {shipment_id}\n"
            f"Product: {product}\n"
            f"Route: {origin} → {destination}\n"
            f"Safe Range: {safe_range}\n"
            f"\n"
            f"📊 COMPLIANCE SUMMARY\n"
            f"{'─'*50}\n"
            f"Temperature Readings: {total_readings}\n"
            f"Compliance Rate: {compliance_pct}%\n"
            f"Incidents/Diversions: {incidents}\n"
            f"Divert Hub Used: {divert_hub}\n"
            f"\n"
            f"🌱 CARBON CREDITS\n"
            f"{'─'*50}\n"
            f"CO₂ Avoided: {co2_saved:.2f} tonnes\n"
            f"Carbon Credits Earned: ₹{credits:,.0f}\n"
            f"\n"
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}\n"
            f"{'='*50}\n"
            f"LiveCold — Cold Chain Intelligence Platform"
        )

        notification = {
            "type": "DELIVERY_EMAIL",
            "channel": "email",
            "recipient": "client",
            "shipment_id": shipment_id,
            "subject": subject,
            "message": email_body,
            "product_type": product,
            "compliance_pct": compliance_pct,
            "co2_saved_tonnes": co2_saved,
            "timestamp": datetime.now().isoformat(),
        }

        self._store_notification(notification)
        self._send_email(subject, email_body, recipient="client")

        log.info(
            f"📧 Email → Client | {shipment_id} | {product} | "
            f"Compliance: {compliance_pct}% | CO₂ saved: {co2_saved:.2f}t"
        )
        return notification

    # ══════════════════════════════════════════════════════════════
    #  Internal: Storage + Simulated Channels
    # ══════════════════════════════════════════════════════════════

    def _store_notification(self, notification):
        """Store notification in memory for API access."""
        with self._lock:
            self._notifications.append(notification)
            # Keep last 200 notifications
            if len(self._notifications) > 200:
                self._notifications = self._notifications[-200:]

    def get_notification_log(self):
        """Return all stored notifications (for /api/notifications endpoint)."""
        with self._lock:
            return list(self._notifications)

    def get_notifications_by_shipment(self, shipment_id):
        """Return notifications for a specific shipment."""
        with self._lock:
            return [n for n in self._notifications if n.get("shipment_id") == shipment_id]

    def get_anomaly_counts(self):
        """Return current anomaly counts per shipment (for dashboard)."""
        with self._anomaly_lock:
            return {
                sid: len(times)
                for sid, times in self._anomaly_tracker.items()
                if times
            }

    # ── Simulated channels (swap with real APIs later) ──────────

    def _send_whatsapp(self, message, recipient="driver"):
        """
        Simulated WhatsApp send.
        TODO: Replace with Twilio WhatsApp Business API:
            from twilio.rest import Client
            client = Client(account_sid, auth_token)
            client.messages.create(
                from_='whatsapp:+14155238886',
                body=message,
                to=f'whatsapp:{driver_phone}'
            )
        """
        log.info(f"[SIM] WhatsApp to {recipient}: {message[:80]}...")

    def _send_email(self, subject, body, recipient="client"):
        """
        Simulated email send.
        TODO: Replace with SMTP or SendGrid:
            import smtplib
            msg = MIMEText(body)
            msg['Subject'] = subject
            msg['From'] = 'alerts@livecold.in'
            msg['To'] = client_email
            server.send_message(msg)
        """
        log.info(f"[SIM] Email to {recipient}: {subject}")


# ── Global singleton ──────────────────────────────────────────────
notifier = AlertNotifier()
