package com.stream.base.model;

/**
 * JetStream üzerinden periyodik yayınlanan ve geçmiş hafızasında saklanan durum verisi.
 * Topic: stream.car.status
 */
public class VehicleStatus {
    public double fuelPercent;
    public double batteryPercent;
    public double odometerKm;
    public double engineTempCelsius;
    public long timestamp;

    public VehicleStatus() {}

    public VehicleStatus(double fuelPercent, double batteryPercent, double odometerKm, double engineTempCelsius) {
        this.fuelPercent = fuelPercent;
        this.batteryPercent = batteryPercent;
        this.odometerKm = odometerKm;
        this.engineTempCelsius = engineTempCelsius;
        this.timestamp = System.currentTimeMillis();
    }

    @Override
    public String toString() {
        return String.format("VehicleStatus[Yakıt=%%%.1f, Batarya=%%%.1f, KM=%.1f, MotorSıcaklığı=%.1f°C]",
                fuelPercent, batteryPercent, odometerKm, engineTempCelsius);
    }
}
