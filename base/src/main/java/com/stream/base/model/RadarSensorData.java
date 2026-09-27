package com.stream.base.model;

/**
 * Dinlenen Konu 2 (NATS Core): Ön tampon radar sensöründen gelen anlık mesafe ve yaklaşma hızı.
 * Topic: core.car.sensors.radar
 */
public class RadarSensorData {
    public double distanceMeters;
    public double relativeSpeedKmh;
    public boolean obstacleDetected;
    public long timestamp;

    public RadarSensorData() {}

    public RadarSensorData(double distanceMeters, double relativeSpeedKmh, boolean obstacleDetected) {
        this.distanceMeters = distanceMeters;
        this.relativeSpeedKmh = relativeSpeedKmh;
        this.obstacleDetected = obstacleDetected;
        this.timestamp = System.currentTimeMillis();
    }

    @Override
    public String toString() {
        return String.format("Radar[Mesafe=%.1fm, BağılHız=%.1f km/h, Engel=%b]",
                distanceMeters, relativeSpeedKmh, obstacleDetected);
    }
}
