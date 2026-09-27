package com.stream.base.model;

/**
 * NATS Core üzerinden acil durum veya ikaz anında yayınlanan veri.
 * Topic: core.car.alerts
 */
public class AlertData {
    public String alertType;   // COLLISION_WARNING, EMERGENCY_BRAKE, ENGINE_OVERHEAT, MAINTENANCE_REQUIRED
    public String severity;    // LOW, MEDIUM, CRITICAL
    public String message;
    public long timestamp;

    public AlertData() {}

    public AlertData(String alertType, String severity, String message) {
        this.alertType = alertType;
        this.severity = severity;
        this.message = message;
        this.timestamp = System.currentTimeMillis();
    }

    @Override
    public String toString() {
        return String.format("Alert[%s - %s: %s]", severity, alertType, message);
    }
}
