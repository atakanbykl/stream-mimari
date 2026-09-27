package com.stream.base.model;

/**
 * NATS Core üzerinden periyodik yayınlanan anlık telemetri verisi (Minimum Gecikme).
 * Topic: core.car.telemetry
 */
public class TelemetryData {
    public double speedKmh;
    public int rpm;
    public double throttlePercent;
    public int currentGear;
    public long timestamp;

    public TelemetryData() {}

    public TelemetryData(double speedKmh, int rpm, double throttlePercent, int currentGear) {
        this.speedKmh = speedKmh;
        this.rpm = rpm;
        this.throttlePercent = throttlePercent;
        this.currentGear = currentGear;
        this.timestamp = System.currentTimeMillis();
    }

    @Override
    public String toString() {
        return String.format("Telemetry[Hız=%.1f km/h, RPM=%d, Gaz=%%%.0f, Vites=%d]",
                speedKmh, rpm, throttlePercent, currentGear);
    }
}
